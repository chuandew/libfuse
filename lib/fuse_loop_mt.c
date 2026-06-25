/*
  FUSE: Filesystem in Userspace
  Copyright (C) 2001-2007  Miklos Szeredi <miklos@szeredi.hu>

  Implementation of the multi-threaded FUSE session loop.

  This program can be distributed under the terms of the GNU LGPLv2.
  See the file LGPL2.txt.
*/

#define _GNU_SOURCE

#include "fuse_config.h"
#include "fuse_lowlevel.h"
#include "fuse_misc.h"
#include "fuse_kernel.h"
#include "fuse_i.h"
#include "fuse_uring_i.h"
#include "util.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <semaphore.h>
#include <errno.h>
#include <sys/time.h>
#include <sys/ioctl.h>
#include <assert.h>
#include <limits.h>

/* Environment var controlling the thread stack size */
#define ENVNAME_THREAD_STACK "FUSE_THREAD_STACK"

#define FUSE_LOOP_MT_V2_IDENTIFIER	 INT_MAX - 2
#define FUSE_LOOP_MT_DEF_CLONE_FD	 0
#define FUSE_LOOP_MT_DEF_MAX_THREADS 10
#define FUSE_LOOP_MT_DEF_IDLE_THREADS -1 /* thread destruction is disabled
                                          * by default */

/* an arbitrary large value that cannot be valid */
#define FUSE_LOOP_MT_MAX_THREADS      (100U * 1000)

struct fuse_worker {
	struct fuse_worker *prev;
	struct fuse_worker *next;
	pthread_t thread_id;

	// We need to include fuse_buf so that we can properly free
	// it when a thread is terminated by pthread_cancel().
	struct fuse_buf fbuf;
	struct fuse_chan *ch;
	struct fuse_mt *mt;
};

/* synchronization via se->mt_lock */
struct fuse_mt {
	int numworker;
	int numavail;
	struct fuse_session *se;
	struct fuse_worker main;
	int error;
	int clone_fd;
	int max_idle;
	int max_threads;
};

static struct fuse_chan *fuse_chan_new(int fd)
{
	struct fuse_chan *ch = (struct fuse_chan *) malloc(sizeof(*ch));
	if (ch == NULL) {
		fuse_log(FUSE_LOG_ERR, "fuse: failed to allocate channel\n");
		return NULL;
	}

	memset(ch, 0, sizeof(*ch));
	ch->fd = fd;
	ch->ctr = 1;
	pthread_mutex_init(&ch->lock, NULL);

	return ch;
}

struct fuse_chan *fuse_chan_get(struct fuse_chan *ch)
{
	assert(ch->ctr > 0);
	pthread_mutex_lock(&ch->lock);
	ch->ctr++;
	pthread_mutex_unlock(&ch->lock);

	return ch;
}

void fuse_chan_put(struct fuse_chan *ch)
{
	if (ch == NULL)
		return;
	pthread_mutex_lock(&ch->lock);
	ch->ctr--;
	if (!ch->ctr) {
		pthread_mutex_unlock(&ch->lock);
		close(ch->fd);
		pthread_mutex_destroy(&ch->lock);
		free(ch);
	} else
		pthread_mutex_unlock(&ch->lock);
}

static void list_add_worker(struct fuse_worker *w, struct fuse_worker *next)
{
	struct fuse_worker *prev = next->prev;
	w->next = next;
	w->prev = prev;
	prev->next = w;
	next->prev = w;
}

static void list_del_worker(struct fuse_worker *w)
{
	struct fuse_worker *prev = w->prev;
	struct fuse_worker *next = w->next;
	prev->next = next;
	next->prev = prev;
}

static int fuse_loop_start_thread(struct fuse_mt *mt);

/*
 * Wake anything blocked in fuse_session_wait_drained(): any change to the
 * drain-relevant counters (reading / received_inflight / parked /
 * exited_workers) must broadcast so the waiter re-evaluates rather than polls.
 */
static void fuse_drain_state_changed(struct fuse_session *se)
{
	pthread_mutex_lock(&se->drain_lock);
	pthread_cond_broadcast(&se->drain_cond);
	pthread_mutex_unlock(&se->drain_lock);
}

static void *fuse_do_work(void *data)
{
	struct fuse_worker *w = (struct fuse_worker *) data;
	struct fuse_mt *mt = w->mt;
	struct fuse_session *se = mt->se;

	fuse_set_thread_name("fuse_worker");

	/*
	 * worker_total is incremented by fuse_loop_start_thread() under mt_lock
	 * when this thread was created, not here: counting on thread entry would
	 * leave a create-vs-pause window (see fuse_loop_start_thread). The
	 * matching exited_workers++ happens at each exit point below.
	 */

#ifdef FUSE_TEST_DRAIN_HOOKS
	/* Test gate: lets a test hold a freshly spawned worker right here, after
	 * fuse_loop_start_thread() returned (so worker_total already counts it)
	 * but before it touches the loop, to prove wait_drained() does not
	 * prematurely report drained for a not-yet-running worker. */
	if (se->test_worker_entry_hook)
		se->test_worker_entry_hook(se);
#endif

	/*
	 * Workers disable cancellation by default; only the /dev/fuse
	 * receive/read window below re-enables it (and disables it again right
	 * after). After receive is paused a worker may park in
	 * pthread_cond_wait(); if it were cancellable there, a cancel would skip
	 * the paired parked--/exited_workers++, corrupt the drain accounting, and
	 * per POSIX could terminate the thread while it holds drain_lock (leaking
	 * it and wedging every other parked worker's join()). A paused worker is
	 * instead woken by the drain_cond broadcast that fuse_session_exit()
	 * issues and exits cleanly. Plain (non-drain) shutdown still interrupts a
	 * worker blocked in read(), because cancellation is enabled in that
	 * window.
	 */
	pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);

	while (!fuse_session_exited(se)) {
		int isforget = 0;
		int res;

		/*
		 * Controlled drain: once receiving is paused, stop taking new
		 * work but do not exit the thread (it must be resumable via
		 * fuse_session_resume_receive()). Park on drain_cond and
		 * re-evaluate when woken by a resume or by exit. Placed before
		 * receive so the pause boundary is "stop before the next
		 * receive"; a worker already blocked inside receive is left to
		 * finish that request.
		 */
		/*
		 * recv_paused is read seq_cst (paired with the seq_cst stores in
		 * fuse_session_pause_receive / fuse_session_resume_receive).
		 * This is NOT what makes the
		 * handoff safe -- the actual drain barrier is the predicate
		 * fuse_session_wait_drained() checks (reading == 0 &&
		 * received_inflight == 0 && parked + exited_workers ==
		 * worker_total). Even if a freshly created worker momentarily read
		 * a stale recv_paused == false here, it is already counted in
		 * worker_total (pre-counted in fuse_loop_start_thread) and is not
		 * yet parked/exited, and reading++ is published before any receive,
		 * so wait_drained() cannot report drained while it is in flight.
		 * seq_cst is kept only to give the flag prompt cross-thread
		 * visibility so paused workers settle quickly and the drain
		 * converges; it is a convergence aid, not the correctness barrier.
		 */
		if (atomic_load_explicit(&se->recv_paused, memory_order_seq_cst) &&
		    !fuse_session_exited(se)) {
			pthread_mutex_lock(&se->drain_lock);
			atomic_fetch_add_explicit(&se->parked, 1,
						  memory_order_relaxed);
			/* parked++ is itself a drain-relevant change. */
			pthread_cond_broadcast(&se->drain_cond);
#ifdef FUSE_TEST_DRAIN_HOOKS
			/* Test hook: this worker is about to cond_wait while
			 * holding drain_lock; let a test grab its tid to cancel
			 * it deterministically inside cond_wait. */
			if (se->test_pre_park_hook)
				se->test_pre_park_hook(se);
#endif
			while (atomic_load_explicit(&se->recv_paused,
						    memory_order_seq_cst) &&
			       !fuse_session_exited(se))
				pthread_cond_wait(&se->drain_cond,
						  &se->drain_lock);
			atomic_fetch_sub_explicit(&se->parked, 1,
						  memory_order_relaxed);
			pthread_cond_broadcast(&se->drain_cond);
			pthread_mutex_unlock(&se->drain_lock);
			continue;
		}

		pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
		/* reading: this worker has entered the receive/read path and
		 * has not yet returned; it is neither parked nor processing. */
		atomic_fetch_add_explicit(&se->reading, 1, memory_order_relaxed);
		fuse_drain_state_changed(se);
		res = fuse_session_receive_buf_internal(se, &w->fbuf, w->ch);
		atomic_fetch_sub_explicit(&se->reading, 1, memory_order_relaxed);
		fuse_drain_state_changed(se);
		pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);
		if (res == -EINTR)
			continue;
		if (res <= 0) {
			if (res < 0) {
				fuse_session_exit(se);
				mt->error = res;
			}
			break;
		}

		/*
		 * Successfully received: this request must be processed and
		 * replied no matter what, even if the session is exiting or
		 * recv_paused. The early "if (exited) return NULL" that used to
		 * sit here dropped already-received requests and is removed.
		 * received_inflight is incremented here (connection-wide:
		 * master and clone workers share se->received_inflight) and
		 * decremented once the request has been processed/replied.
		 *
		 * No broadcast on the increment (unlike the decrement below): an
		 * increment can only move the state further from the drained
		 * predicate (inflight == 0), so no wait_drained() waiter could
		 * newly become satisfied by it -- waking them would be spurious.
		 */
		atomic_fetch_add_explicit(&se->received_inflight, 1,
					  memory_order_relaxed);

#ifdef FUSE_TEST_DRAIN_HOOKS
		if (se->test_after_receive_hook)
			se->test_after_receive_hook(se);
#endif

		pthread_mutex_lock(&se->mt_lock);

		/*
		 * This disgusting hack is needed so that zillions of threads
		 * are not created on a burst of FORGET messages
		 */
		if (!(w->fbuf.flags & FUSE_BUF_IS_FD)) {
			struct fuse_in_header *in = w->fbuf.mem;

			if (in->opcode == FUSE_FORGET ||
			    in->opcode == FUSE_BATCH_FORGET)
				isforget = 1;
		}

		if (!isforget)
			mt->numavail--;
		/* Do not grow the worker pool once recv_paused: keeping
		 * worker_total fixed is what makes the wait_drained condition
		 * provable. The already-received request below is unaffected. */
		if (mt->numavail == 0 && mt->numworker < mt->max_threads &&
		    likely(se->got_init) &&
		    !atomic_load_explicit(&se->recv_paused, memory_order_seq_cst))
			fuse_loop_start_thread(mt);
		pthread_mutex_unlock(&se->mt_lock);

		fuse_session_process_buf_internal(se, &w->fbuf, w->ch);
		/* Request has been replied (or was a FORGET needing no reply):
		 * pair the received_inflight increment from above. */
		atomic_fetch_sub_explicit(&se->received_inflight, 1,
					  memory_order_relaxed);
		fuse_drain_state_changed(se);

		pthread_mutex_lock(&se->mt_lock);
		if (!isforget)
			mt->numavail++;

		/* creating and destroying threads is rather expensive - and there is
		 * not much gain from destroying existing threads. It is therefore
		 * discouraged to set max_idle to anything else than -1. If there
		 * is indeed a good reason to destruct threads it should be done
		 * delayed, a moving average might be useful for that.
		 */
		if (mt->max_idle != -1 && mt->numavail > mt->max_idle && mt->numworker > 1) {
			if (fuse_session_exited(se)) {
				/* Safe: this runs after process_buf_internal,
				 * so received_inflight was already decremented
				 * and no received request is dropped. */
				pthread_mutex_unlock(&se->mt_lock);
				atomic_fetch_add_explicit(&se->exited_workers, 1,
							  memory_order_relaxed);
				fuse_drain_state_changed(se);
				return NULL;
			}
			list_del_worker(w);
			mt->numavail--;
			mt->numworker--;
			pthread_mutex_unlock(&se->mt_lock);

			pthread_detach(w->thread_id);
			fuse_buf_free(&w->fbuf);
			fuse_chan_put(w->ch);
			free(w);
			atomic_fetch_add_explicit(&se->exited_workers, 1,
						  memory_order_relaxed);
			fuse_drain_state_changed(se);
			return NULL;
		}
		pthread_mutex_unlock(&se->mt_lock);
	}

	atomic_fetch_add_explicit(&se->exited_workers, 1, memory_order_relaxed);
	fuse_drain_state_changed(se);
	sem_post(&se->mt_finish);
	return NULL;
}

int fuse_start_thread(pthread_t *thread_id, void *(*func)(void *), void *arg)
{
	sigset_t oldset;
	sigset_t newset;
	int res;
	pthread_attr_t attr;
	char *stack_size;

	/* Override default stack size
	 * XXX: This should ideally be a parameter option. It is rather
	 *      well hidden here.
	 */
	pthread_attr_init(&attr);
	stack_size = getenv(ENVNAME_THREAD_STACK);
	if (stack_size) {
		long size;

		res = libfuse_strtol(stack_size, &size);
		if (res)
			fuse_log(FUSE_LOG_ERR, "fuse: invalid stack size: %s\n",
				 stack_size);
		else if (pthread_attr_setstacksize(&attr, size))
			fuse_log(FUSE_LOG_ERR, "fuse: could not set stack size: %ld\n",
				 size);
	}

	/* Disallow signal reception in worker threads */
	sigemptyset(&newset);
	sigaddset(&newset, SIGTERM);
	sigaddset(&newset, SIGINT);
	sigaddset(&newset, SIGHUP);
	sigaddset(&newset, SIGQUIT);
	pthread_sigmask(SIG_BLOCK, &newset, &oldset);
	res = pthread_create(thread_id, &attr, func, arg);
	pthread_sigmask(SIG_SETMASK, &oldset, NULL);
	pthread_attr_destroy(&attr);
	if (res != 0) {
		fuse_log(FUSE_LOG_ERR, "fuse: error creating thread: %s\n",
			strerror(res));
		return -1;
	}

	return 0;
}

static int fuse_clone_chan_fd_default(struct fuse_session *se)
{
	int res;
	int clonefd;
	uint32_t masterfd;
	const char *devname = "/dev/fuse";

#ifndef O_CLOEXEC
#define O_CLOEXEC 0
#endif
	clonefd = open(devname, O_RDWR | O_CLOEXEC);
	if (clonefd == -1) {
		fuse_log(FUSE_LOG_ERR, "fuse: failed to open %s: %s\n", devname,
			strerror(errno));
		return -1;
	}
	if (!O_CLOEXEC) {
		res = fcntl(clonefd, F_SETFD, FD_CLOEXEC);
		if (res == -1) {
			fuse_log(FUSE_LOG_ERR, "fuse: failed to set CLOEXEC: %s\n",
				strerror(errno));
			close(clonefd);
			return -1;
		}
	}

	masterfd = se->fd;
	res = ioctl(clonefd, FUSE_DEV_IOC_CLONE, &masterfd);
	if (res == -1) {
		fuse_log(FUSE_LOG_ERR, "fuse: failed to clone device fd: %s\n",
			strerror(errno));
		close(clonefd);
		return -1;
	}
	return clonefd;
}

static struct fuse_chan *fuse_clone_chan(struct fuse_mt *mt)
{
	int clonefd;
	struct fuse_session *se = mt->se;
	struct fuse_chan *newch;

	if (se->io != NULL) {
		if (se->io->clone_fd != NULL)
			clonefd = se->io->clone_fd(se->fd);
		else
			return NULL;
	} else {
		clonefd = fuse_clone_chan_fd_default(se);
	}
	if (clonefd < 0)
		return NULL;

	newch = fuse_chan_new(clonefd);
	if (newch == NULL)
		close(clonefd);

	return newch;
}

static int fuse_loop_start_thread(struct fuse_mt *mt)
{
	int res;

	struct fuse_worker *w = malloc(sizeof(struct fuse_worker));
	if (!w) {
		fuse_log(FUSE_LOG_ERR, "fuse: failed to allocate worker structure\n");
		return -1;
	}
	memset(w, 0, sizeof(struct fuse_worker));
	w->fbuf.mem = NULL;
	w->mt = mt;

	w->ch = NULL;
	if (mt->clone_fd) {
		w->ch = fuse_clone_chan(mt);
		if(!w->ch) {
			/* Don't attempt this again */
			fuse_log(FUSE_LOG_ERR, "fuse: trying to continue "
				"without -o clone_fd.\n");
			mt->clone_fd = 0;
		}
	}

	/*
	 * worker_total must be counted BEFORE thread creation. Otherwise a newly
	 * created worker not yet in the thread body could let wait_drained()
	 * observe total too small, wrongly report drained, and hand off before
	 * that worker can stop receiving. wait_drained()/is_drained() take
	 * drain_lock (not mt_lock), so they have no mutual exclusion with worker
	 * creation; pre-counting only ever biases is_drained() toward NOT drained
	 * (the safe direction). Roll back on create failure (thread exists =>
	 * counted). Done under mt_lock, in the same critical section as
	 * numworker++ and the !recv_paused growth guard.
	 */
	atomic_fetch_add_explicit(&mt->se->worker_total, 1, memory_order_relaxed);

	res = fuse_start_thread(&w->thread_id, fuse_do_work, w);
	if (res == -1) {
		/* Roll back the pre-count: no thread was created. Broadcast so a
		 * concurrent wait_drained() re-evaluates against the lowered
		 * total. */
		atomic_fetch_sub_explicit(&mt->se->worker_total, 1,
					  memory_order_relaxed);
		fuse_drain_state_changed(mt->se);
		fuse_chan_put(w->ch);
		free(w);
		return -1;
	}
	list_add_worker(w, &mt->main);
	mt->numavail ++;
	mt->numworker ++;

	return 0;
}

static void fuse_join_worker(struct fuse_mt *mt, struct fuse_worker *w)
{
	pthread_join(w->thread_id, NULL);
	pthread_mutex_lock(&mt->se->mt_lock);
	list_del_worker(w);
	pthread_mutex_unlock(&mt->se->mt_lock);
	fuse_buf_free(&w->fbuf);
	fuse_chan_put(w->ch);
	free(w);
}

int fuse_session_loop_mt_312(struct fuse_session *se, struct fuse_loop_config *config);
FUSE_SYMVER("fuse_session_loop_mt_312", "fuse_session_loop_mt@@FUSE_3.12")
int fuse_session_loop_mt_312(struct fuse_session *se, struct fuse_loop_config *config)
{
int err;
	struct fuse_mt mt;
	struct fuse_worker *w;
	int created_config = 0;

	if (config) {
		err = fuse_loop_cfg_verify(config);
		if (err)
			return err;
	} else {
		/* The caller does not care about parameters - use the default */
		config = fuse_loop_cfg_create();
		created_config = 1;
	}


	memset(&mt, 0, sizeof(struct fuse_mt));
	mt.se = se;
	mt.clone_fd = config->clone_fd;
	mt.error = 0;
	mt.numworker = 0;
	mt.numavail = 0;
	mt.max_idle = config->max_idle_threads;
	mt.max_threads = config->max_threads;
	mt.main.thread_id = pthread_self();
	mt.main.prev = mt.main.next = &mt.main;

	pthread_mutex_lock(&se->mt_lock);
	err = fuse_loop_start_thread(&mt);
	pthread_mutex_unlock(&se->mt_lock);
	if (!err) {
		while (!fuse_session_exited(se))
			sem_wait(&se->mt_finish);
		if (se->debug)
			fuse_log(FUSE_LOG_DEBUG,
				 "fuse: session exited, terminating workers\n");

		/*
		 * Two distinct teardown paths must not be conflated:
		 *
		 *  - Drain (hot-upgrade): fuse_session_pause_receive() + statfs wakeup +
		 *    fuse_session_wait_drained() bring every worker to a parked
		 *    point with no request in flight and nothing in read; the
		 *    orchestrator hands off only after that. No worker is cancelled
		 *    mid-process, so no received request is dropped.
		 *
		 *  - Exit (this stock path): fuse_session_exit() was called. We
		 *    reach a worker only once it is back at the top of its loop
		 *    (cancellation is enabled solely around receive, disabled
		 *    across process_buf), so pthread_cancel() here cannot abort a
		 *    request that has been received but not yet replied -- the
		 *    early "drop received request on exit" path was removed. This
		 *    cancel is just the stock wakeup for workers blocked in read()
		 *    on a plain (non-drain) shutdown; the hot-upgrade EXITING path
		 *    instead relies on received_inflight reaching 0 and the workers
		 *    self-exiting + join, never on forced cancellation.
		 */
		pthread_mutex_lock(&se->mt_lock);
		for (w = mt.main.next; w != &mt.main; w = w->next)
			pthread_cancel(w->thread_id);
		pthread_mutex_unlock(&se->mt_lock);

		while (mt.main.next != &mt.main)
			fuse_join_worker(&mt, mt.main.next);

		err = mt.error;

		if (se->uring.pool)
			fuse_uring_stop(se);
	}

	pthread_mutex_destroy(&se->mt_lock);
	if(se->error != 0)
		err = se->error;


	if (created_config) {
		fuse_loop_cfg_destroy(config);
		config = NULL;
	}

	return err;
}

int fuse_session_loop_mt_32(struct fuse_session *se, struct fuse_loop_config_v1 *config_v1);
FUSE_SYMVER("fuse_session_loop_mt_32", "fuse_session_loop_mt@FUSE_3.2")
int fuse_session_loop_mt_32(struct fuse_session *se, struct fuse_loop_config_v1 *config_v1)
{
	int err;
	struct fuse_loop_config *config = NULL;

	if (config_v1 != NULL) {
		/* convert the given v1 config */
		config = fuse_loop_cfg_create();
		if (config == NULL)
			return ENOMEM;

		fuse_loop_cfg_convert(config, config_v1);
	}

	err = fuse_session_loop_mt_312(se, config);

	fuse_loop_cfg_destroy(config);

	return err;
}


int fuse_session_loop_mt_31(struct fuse_session *se, int clone_fd);
FUSE_SYMVER("fuse_session_loop_mt_31", "fuse_session_loop_mt@FUSE_3.0")
int fuse_session_loop_mt_31(struct fuse_session *se, int clone_fd)
{
	int err;
	struct fuse_loop_config *config = fuse_loop_cfg_create();
	if (clone_fd > 0)
		 fuse_loop_cfg_set_clone_fd(config, clone_fd);
	err = fuse_session_loop_mt_312(se, config);

	fuse_loop_cfg_destroy(config);

	return err;
}

struct fuse_loop_config *fuse_loop_cfg_create(void)
{
	struct fuse_loop_config *config = calloc(1, sizeof(*config));
	if (config == NULL)
		return NULL;

	config->version_id       = FUSE_LOOP_MT_V2_IDENTIFIER;
	config->max_idle_threads = FUSE_LOOP_MT_DEF_IDLE_THREADS;
	config->max_threads      = FUSE_LOOP_MT_DEF_MAX_THREADS;
	config->clone_fd         = FUSE_LOOP_MT_DEF_CLONE_FD;

	return config;
}

void fuse_loop_cfg_destroy(struct fuse_loop_config *config)
{
	free(config);
}

int fuse_loop_cfg_verify(struct fuse_loop_config *config)
{
	if (config->version_id != FUSE_LOOP_MT_V2_IDENTIFIER)
		return -EINVAL;

	return 0;
}

void fuse_loop_cfg_convert(struct fuse_loop_config *config,
			   struct fuse_loop_config_v1 *v1_conf)
{
	fuse_loop_cfg_set_idle_threads(config, v1_conf->max_idle_threads);

	fuse_loop_cfg_set_clone_fd(config, v1_conf->clone_fd);
}

void fuse_loop_cfg_set_idle_threads(struct fuse_loop_config *config,
				    unsigned int value)
{
	if (value > FUSE_LOOP_MT_MAX_THREADS) {
		if (value != UINT_MAX)
			fuse_log(FUSE_LOG_ERR,
				 "Ignoring invalid max threads value "
				 "%u > max (%u).\n", value,
				 FUSE_LOOP_MT_MAX_THREADS);
		return;
	}
	config->max_idle_threads = value;
}

void fuse_loop_cfg_set_max_threads(struct fuse_loop_config *config,
				   unsigned int value)
{
	config->max_threads = value;
}

void fuse_loop_cfg_set_clone_fd(struct fuse_loop_config *config,
				unsigned int value)
{
	config->clone_fd = value;
}

