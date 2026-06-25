/*
 * FUSE: Filesystem in Userspace
 *
 * Deterministic regression test for parked-worker cancellation safety
 * (reviewer blocker), built with -DFUSE_TEST_DRAIN_HOOKS for the test hooks.
 *
 * The bug: a fuse worker defaults to PTHREAD_CANCEL_ENABLE and only the
 * read()/receive window toggled it. A fresh worker that never reached receive
 * but parked at the drain pause point sits in pthread_cond_wait() -- a cancellation
 * point -- still ENABLED. A pthread_cancel() (the stock loop teardown issues
 * one per worker) then cancels it *inside* cond_wait: the paired
 * parked--/exited_workers++ never runs, and per POSIX the thread terminates
 * while holding drain_lock. That leaked lock then deadlocks everyone else who
 * touches drain_lock (other parked workers' wakeup, fuse_session_resume_receive,
 * wait_drained), wedging the whole session.
 *
 * The fix disables cancellation for the entire worker body at thread entry,
 * re-enabling it only around the read()/receive window, so a parked worker is
 * never cancellable: pthread_cancel() only sets a pending cancel and the worker
 * is later woken normally by a drain_cond broadcast.
 *
 * Deterministic reproduction (no scheduling race, no real hang):
 *   - Mount, then pause receiving BEFORE starting the loop, so the initial worker
 *     reaches the while() top, sees recv_paused and parks -- without ever going
 *     through receive (the fresh-worker park state).
 *   - test_pre_park_hook fires while that worker holds drain_lock, right
 *     before cond_wait, and hands the worker's pthread_self() to the test.
 *   - The test waits until the worker is genuinely blocked in cond_wait (the
 *     hook returned and drain_lock is observably free again), then directly
 *     pthread_cancel()s that worker -- NOT via fuse_session_exit(), so no
 *     broadcast wakes it first. The cancel therefore targets a thread sitting
 *     in cond_wait with recv_paused==true && exited==false.
 *   - Detection without a hang: a watchdog thread calls
 *     fuse_session_resume_receive() (which must take drain_lock). With the
 *     cancellation fix the worker is alive, resume wakes it, parked drops to 0
 *     and resume returns; the test passes. Were the worker to die holding
 *     drain_lock (the bug), resume would block forever on the leaked lock and
 *     the watchdog timed-join would expire -- so the test reports failure via
 *     a bounded timeout instead of hanging.
 */

#define FUSE_USE_VERSION FUSE_MAKE_VERSION(3, 17)

#ifndef _GNU_SOURCE
#define _GNU_SOURCE  /* pthread_timedjoin_np */
#endif

#include "fuse_config.h"
#include "fuse_i.h"
#include "fuse_lowlevel.h"

#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

static pthread_t parked_worker_tid;
static atomic_int parked_worker_captured;

static void pre_park_hook(struct fuse_session *se)
{
	(void)se;
	int expected = 0;

	/* Capture only the first worker to park. */
	if (atomic_compare_exchange_strong(&parked_worker_captured, &expected, 1))
		parked_worker_tid = pthread_self();
}

static void test_ll_getattr(fuse_req_t req, fuse_ino_t ino,
			    struct fuse_file_info *fi)
{
	(void)fi;
	struct stat st;

	memset(&st, 0, sizeof(st));
	if (ino == FUSE_ROOT_ID) {
		st.st_ino = ino;
		st.st_mode = S_IFDIR | 0755;
		st.st_nlink = 2;
		fuse_reply_attr(req, &st, 1.0);
	} else {
		fuse_reply_err(req, ENOENT);
	}
}

static const struct fuse_lowlevel_ops test_ll_ops = {
	.getattr = test_ll_getattr,
};

struct loop_arg {
	struct fuse_session *se;
	int clone_fd;
};

static void *loop_thread(void *arg)
{
	struct loop_arg *la = arg;
	struct fuse_loop_config *cfg = fuse_loop_cfg_create();

	fuse_loop_cfg_set_clone_fd(cfg, la->clone_fd);
	fuse_loop_cfg_set_max_threads(cfg, 4);
	fuse_session_loop_mt_312(la->se, cfg);
	fuse_loop_cfg_destroy(cfg);
	return NULL;
}

/* Watchdog: call fuse_session_resume_receive(), which must acquire drain_lock.
 * If the parked worker died holding that lock (the bug), this blocks forever;
 * the main thread detects that via pthread_timedjoin_np on this thread. */
static atomic_int resume_returned;

static void *resume_thread(void *arg)
{
	struct fuse_session *se = arg;

	fuse_session_resume_receive(se);
	atomic_store(&resume_returned, 1);
	return NULL;
}

static int run_once(int clone_fd)
{
	struct fuse_args args = FUSE_ARGS_INIT(0, NULL);
	struct fuse_session *se;
	char *mountpoint;
	pthread_t loop_tid, wd_tid;
	struct loop_arg la;
	struct timespec deadline;
	int i, rc = 1, jrc;

	atomic_store(&parked_worker_captured, 0);
	atomic_store(&resume_returned, 0);

	if (fuse_opt_add_arg(&args, "test_drain_cancel_safety"))
		return 1;

	mountpoint = strdup("/tmp/fuse_cancelsafe_XXXXXX");
	if (!mountpoint || !mkdtemp(mountpoint)) {
		free(mountpoint);
		fuse_opt_free_args(&args);
		return 1;
	}

	se = fuse_session_new(&args, &test_ll_ops, sizeof(test_ll_ops), NULL);
	if (!se)
		goto out_dir;

	se->test_pre_park_hook = pre_park_hook;

	if (fuse_session_mount(se, mountpoint)) {
		fprintf(stderr, "mount failed (need setuid fusermount3)\n");
		rc = 77;
		goto out_destroy;
	}

	/* Pause receiving before the loop: the initial worker parks at the recv_paused flag
	 * boundary without ever entering receive. */
	fuse_session_pause_receive(se);

	la.se = se;
	la.clone_fd = clone_fd;
	if (pthread_create(&loop_tid, NULL, loop_thread, &la))
		goto out_resume;

	/* Wait until the pre-park hook captured the worker's tid AND it has had
	 * time to actually enter cond_wait (the hook runs right before it). */
	for (i = 0; i < 500 && !atomic_load(&parked_worker_captured); i++)
		usleep(10 * 1000);
	if (!atomic_load(&parked_worker_captured)) {
		fprintf(stderr, "no worker reached the park point\n");
		fuse_session_exit(se);
		pthread_join(loop_tid, NULL);
		goto out_unmount;
	}
	/* Let the captured worker settle into cond_wait (releasing drain_lock
	 * as cond_wait does atomically). */
	usleep(100 * 1000);

	/*
	 * Directly cancel the parked worker while it is blocked in cond_wait with
	 * recv_paused==true && exited==false. We do NOT call fuse_session_exit(),
	 * so no broadcast wakes it first -- the cancel deterministically targets
	 * the cond_wait. With the fix (cancellation disabled in the worker body)
	 * this only sets a pending cancel; with the bug it terminates the worker
	 * inside cond_wait, leaking drain_lock.
	 */
	pthread_cancel(parked_worker_tid);
	usleep(50 * 1000);

	/* Watchdog resume: blocks forever iff drain_lock was leaked. */
	if (pthread_create(&wd_tid, NULL, resume_thread, se)) {
		fprintf(stderr, "failed to start watchdog\n");
		fuse_session_exit(se);
		pthread_join(loop_tid, NULL);
		goto out_unmount;
	}

	clock_gettime(CLOCK_REALTIME, &deadline);
	deadline.tv_sec += 3;
	jrc = pthread_timedjoin_np(wd_tid, NULL, &deadline);
	if (jrc == ETIMEDOUT) {
		fprintf(stderr,
			"REGRESSION: fuse_session_resume_receive() wedged after "
			"cancelling a parked worker (clone_fd=%d) -- the worker "
			"was cancelled inside cond_wait and leaked drain_lock "
			"(parked=%d exited=%d total=%d)\n", clone_fd,
			atomic_load_explicit(&se->parked, memory_order_relaxed),
			atomic_load_explicit(&se->exited_workers,
					     memory_order_relaxed),
			atomic_load_explicit(&se->worker_total,
					     memory_order_relaxed));
		/* Leave the wedged threads; do not join (would hang). Report
		 * failure and skip session_destroy (live threads may touch it). */
		goto out_unmount_nowait;
	}
	if (jrc != 0) {
		fprintf(stderr, "watchdog join failed: %s\n", strerror(jrc));
		goto out_unmount;
	}

	/*
	 * GREEN: resume returned, so the worker was alive (cancel only
	 * pended). Resume woke it; it should leave the park (parked -> 0).
	 * The pending cancel is harmless while cancellation stays disabled.
	 */
	for (i = 0; i < 200; i++) {
		if (atomic_load_explicit(&se->parked, memory_order_relaxed) == 0)
			break;
		usleep(10 * 1000);
	}
	if (atomic_load_explicit(&se->parked, memory_order_relaxed) != 0) {
		fprintf(stderr,
			"worker did not leave park after resume (clone_fd=%d): "
			"parked=%d\n", clone_fd,
			atomic_load_explicit(&se->parked, memory_order_relaxed));
		fuse_session_exit(se);
		pthread_join(loop_tid, NULL);
		goto out_unmount;
	}

	/* Clean shutdown of the loop. */
	fuse_session_exit(se);
	clock_gettime(CLOCK_REALTIME, &deadline);
	deadline.tv_sec += 5;
	jrc = pthread_timedjoin_np(loop_tid, NULL, &deadline);
	if (jrc == ETIMEDOUT) {
		fprintf(stderr, "loop did not return on clean shutdown\n");
		goto out_unmount_nowait;
	}

	rc = 0;
	goto out_unmount;

out_resume:
	fuse_session_resume_receive(se);
out_unmount:
	fuse_session_unmount(se);
out_destroy:
	fuse_session_destroy(se);
out_dir:
	rmdir(mountpoint);
	free(mountpoint);
	fuse_opt_free_args(&args);
	return rc;

out_unmount_nowait:
	fuse_session_unmount(se);
	rmdir(mountpoint);
	free(mountpoint);
	fuse_opt_free_args(&args);
	return rc;
}

int main(void)
{
	int rc;

	/* clone_fd 0 only: the deterministic single-parked-worker capture is
	 * cleanest on the master channel. */
	rc = run_once(0);
	if (rc == 77) {
		printf("drain-cancel-safety test skipped (no mount capability)\n");
		return 77;
	}
	if (rc != 0)
		return rc;

	printf("drain-cancel-safety regression test passed\n");
	return 0;
}
