/*
 * FUSE: Filesystem in Userspace
 *
 * Functional test for the controlled-drain API:
 *   fuse_session_pause_receive / fuse_session_resume_receive /
 *   fuse_session_received_inflight / fuse_session_wait_drained.
 *
 * run_pause(): mounts a minimal low-level FS whose lookup blocks on a barrier
 * the test controls, drives stat()s over the mount from client threads, and
 * asserts:
 *   - received_inflight reflects a received-but-not-replied request,
 *   - while that request is in flight, wait_drained() times out (<0): proving
 *     received_inflight==0 is necessary for the drained point,
 *   - after pause a NEW request is not served (workers stop receiving) while
 *     the in-flight one still completes and drains to 0,
 *   - the drained request received the correct reply (ENOENT),
 *   - resume lets the held-back request be served again, also replying
 *     ENOENT.
 * Runs once over the master channel and once with clone_fd enabled, so the
 * connection-wide aggregation is exercised on both.
 *
 * run_wait_drained(): mounts a non-blocking FS, lets the worker pool settle,
 * pauses receiving with NO request pending, and asserts wait_drained() reaches 0 once
 * every worker has parked at the drain pause boundary (parked + exited_workers ==
 * worker_total, reading == 0, received_inflight == 0). statfs round-trips are
 * driven to pop any worker blocked in read() back to the recv_paused check, exactly
 * as the orchestration side would. Kept separate from run_pause() so the
 * blocked-read wakeup is not racing a deliberately-held request.
 *
 * Needs a working mount (setuid fusermount3); exits 77 to skip otherwise.
 */

#define FUSE_USE_VERSION FUSE_MAKE_VERSION(3, 17)

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
#include <sys/statvfs.h>
#include <sys/types.h>
#include <unistd.h>

/* Gate that lookup handlers block on, so the test can hold a request
 * in-flight. */
static pthread_mutex_t gate_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t gate_cond = PTHREAD_COND_INITIALIZER;
static int gate_open;
static atomic_int handlers_entered;

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

/* Blocking lookup: each lookup blocks on the gate. The kernel serialises
 * lookups under the same parent, so the test holds a single request in flight
 * at a time. */
static void test_ll_lookup_gated(fuse_req_t req, fuse_ino_t parent,
				 const char *name)
{
	(void)parent;
	(void)name;

	atomic_fetch_add(&handlers_entered, 1);

	pthread_mutex_lock(&gate_lock);
	while (!gate_open)
		pthread_cond_wait(&gate_cond, &gate_lock);
	pthread_mutex_unlock(&gate_lock);

	fuse_reply_err(req, ENOENT);
}

/* Non-blocking lookup for the wait_drained scenario. */
static void test_ll_lookup_fast(fuse_req_t req, fuse_ino_t parent,
				const char *name)
{
	(void)parent;
	(void)name;
	fuse_reply_err(req, ENOENT);
}

static const struct fuse_lowlevel_ops gated_ops = {
	.lookup = test_ll_lookup_gated,
	.getattr = test_ll_getattr,
};

static const struct fuse_lowlevel_ops fast_ops = {
	.lookup = test_ll_lookup_fast,
	.getattr = test_ll_getattr,
};

static void set_gate(int open)
{
	pthread_mutex_lock(&gate_lock);
	gate_open = open;
	if (open)
		pthread_cond_broadcast(&gate_cond);
	pthread_mutex_unlock(&gate_lock);
}

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

struct stat_result {
	char path[256];
	int ret;
	int err;
};

static void *stat_thread(void *arg)
{
	struct stat_result *r = arg;
	struct stat st;

	errno = 0;
	r->ret = stat(r->path, &st); /* expected -1/ENOENT (lookup -> ENOENT) */
	r->err = errno;
	return NULL;
}

static int wait_handlers(int n)
{
	int i;

	for (i = 0; i < 500 && atomic_load(&handlers_entered) < n; i++)
		usleep(10 * 1000);
	return atomic_load(&handlers_entered) >= n;
}

static int wait_inflight_zero(struct fuse_session *se)
{
	int i;

	for (i = 0; i < 1000 && fuse_session_received_inflight(se) != 0; i++)
		usleep(10 * 1000);
	return fuse_session_received_inflight(se) == 0;
}

/* pause / received_inflight / reply scenario.
 * Returns 0 pass, 77 skip, else fail. */
static int run_pause(int clone_fd)
{
	struct fuse_args args = FUSE_ARGS_INIT(0, NULL);
	struct fuse_session *se;
	char *mountpoint;
	pthread_t loop_tid, c0, c1;
	struct stat_result r0, r1;
	struct loop_arg la;
	int rc = 1;

	set_gate(0);
	atomic_store(&handlers_entered, 0);

	if (fuse_opt_add_arg(&args, "test_received_inflight"))
		return 1;

	mountpoint = strdup("/tmp/fuse_recvinflight_XXXXXX");
	if (!mountpoint || !mkdtemp(mountpoint)) {
		free(mountpoint);
		fuse_opt_free_args(&args);
		return 1;
	}

	se = fuse_session_new(&args, &gated_ops, sizeof(gated_ops), NULL);
	if (!se)
		goto out_dir;

	if (fuse_session_mount(se, mountpoint)) {
		fprintf(stderr, "mount failed (need setuid fusermount3)\n");
		rc = 77;
		goto out_destroy;
	}

	la.se = se;
	la.clone_fd = clone_fd;
	if (pthread_create(&loop_tid, NULL, loop_thread, &la))
		goto out_unmount;

	snprintf(r0.path, sizeof(r0.path), "%s/name0", mountpoint);
	snprintf(r1.path, sizeof(r1.path), "%s/name1", mountpoint);
	r0.ret = r1.ret = 0;
	r0.err = r1.err = 0;

	/* First request: held in lookup, one in flight. */
	if (pthread_create(&c0, NULL, stat_thread, &r0))
		goto out_release;
	if (!wait_handlers(1)) {
		fprintf(stderr, "first request never reached handler\n");
		goto out_release_join0;
	}
	if (fuse_session_received_inflight(se) != 1) {
		fprintf(stderr, "received_inflight expected 1, got %d\n",
			fuse_session_received_inflight(se));
		goto out_release_join0;
	}

	/* With a request still in flight, the drain point must NOT be claimed:
	 * wait_drained() has to time out because received_inflight != 0. */
	fuse_session_pause_receive(se);
	if (fuse_session_wait_drained(se, 200) == 0) {
		fprintf(stderr, "wait_drained returned 0 while a request was "
			"in flight (received_inflight=%d)\n",
			fuse_session_received_inflight(se));
		goto out_release_join0;
	}

	/* recv_paused is set: issue a second stat; its lookup must NOT enter a
	 * handler while paused (workers stop receiving new requests). */
	if (pthread_create(&c1, NULL, stat_thread, &r1))
		goto out_release_join0;

	usleep(300 * 1000);
	if (atomic_load(&handlers_entered) != 1) {
		fprintf(stderr, "new request served while paused (entered=%d)\n",
			atomic_load(&handlers_entered));
		goto out_release_join01;
	}

	/* Release the gate: the in-flight request completes; drain to 0. The
	 * second request is still parked in the kernel (workers paused). */
	set_gate(1);
	if (!wait_inflight_zero(se)) {
		fprintf(stderr, "drain did not reach 0 (clone_fd=%d): %d\n",
			clone_fd, fuse_session_received_inflight(se));
		goto out_release_join01;
	}
	pthread_join(c0, NULL);

	/* The drained in-flight request must not merely have been released: it
	 * must have received the correct reply. Our lookup replies ENOENT, so
	 * the client stat must be -1 with errno ENOENT. */
	if (r0.ret != -1 || r0.err != ENOENT) {
		fprintf(stderr,
			"drained request got wrong reply: ret=%d errno=%d\n",
			r0.ret, r0.err);
		goto out_release_join1;
	}

	/* Resume: the parked second request is now served (gate already
	 * open). */
	fuse_session_resume_receive(se);
	if (!wait_handlers(2)) {
		fprintf(stderr, "request not served after resume\n");
		goto out_release_join1;
	}
	pthread_join(c1, NULL);

	/* The second request, served after resume, must also reply ENOENT. */
	if (r1.ret != -1 || r1.err != ENOENT) {
		fprintf(stderr,
			"post-resume request got wrong reply: ret=%d errno=%d\n",
			r1.ret, r1.err);
		goto out_unmount_exit;
	}

	fuse_session_exit(se);
	pthread_join(loop_tid, NULL);
	rc = 0;
	goto out_unmount;

out_unmount_exit:
	/* clients already joined; just stop the loop and clean up */
	fuse_session_exit(se);
	pthread_join(loop_tid, NULL);
	goto out_unmount;
out_release_join01:
	set_gate(1);
	pthread_join(c1, NULL);
out_release_join0:
	set_gate(1);
	pthread_join(c0, NULL);
	fuse_session_exit(se);
	pthread_join(loop_tid, NULL);
	goto out_unmount;
out_release_join1:
	set_gate(1);
	pthread_join(c1, NULL);
	fuse_session_exit(se);
	pthread_join(loop_tid, NULL);
	goto out_unmount;
out_release:
	set_gate(1);
	fuse_session_exit(se);
	pthread_join(loop_tid, NULL);
out_unmount:
	fuse_session_unmount(se);
out_destroy:
	fuse_session_destroy(se);
out_dir:
	rmdir(mountpoint);
	free(mountpoint);
	fuse_opt_free_args(&args);
	return rc;
}

/* wait_drained / parked scenario: pause receiving on an idle session (no held request) and
 * confirm wait_drained() reaches the drained point once every worker parks.
 * Returns 0 pass, 77 skip, else fail. */
static int run_wait_drained(int clone_fd)
{
	struct fuse_args args = FUSE_ARGS_INIT(0, NULL);
	struct fuse_session *se;
	char *mountpoint;
	pthread_t loop_tid;
	struct loop_arg la;
	struct stat st;
	struct statvfs vfs;
	int i, drained = -1, rc = 1;

	if (fuse_opt_add_arg(&args, "test_received_inflight"))
		return 1;

	mountpoint = strdup("/tmp/fuse_waitdrain_XXXXXX");
	if (!mountpoint || !mkdtemp(mountpoint)) {
		free(mountpoint);
		fuse_opt_free_args(&args);
		return 1;
	}

	se = fuse_session_new(&args, &fast_ops, sizeof(fast_ops), NULL);
	if (!se)
		goto out_dir;

	if (fuse_session_mount(se, mountpoint)) {
		fprintf(stderr, "mount failed (need setuid fusermount3)\n");
		rc = 77;
		goto out_destroy;
	}

	la.se = se;
	la.clone_fd = clone_fd;
	if (pthread_create(&loop_tid, NULL, loop_thread, &la))
		goto out_unmount;

	/* Drive a couple of round-trips so the worker pool is fully spun up and
	 * the INIT handshake is done, then let workers settle into blocking
	 * read(). */
	stat(mountpoint, &st);
	stat(mountpoint, &st);
	usleep(100 * 1000);

	/* No request is in flight now. Pause receiving, then drive statfs round-trips to
	 * pop any worker blocked in read() back to the recv_paused check so it
	 * parks; poll wait_drained() until the drained point is reached. */
	fuse_session_pause_receive(se);
	for (i = 0; i < 100; i++) {
		statvfs(mountpoint, &vfs);
		drained = fuse_session_wait_drained(se, 100);
		if (drained == 0)
			break;
	}

	if (drained != 0) {
		fprintf(stderr,
			"wait_drained never reached drained point (clone_fd=%d): "
			"reading=%d received_inflight=%d parked=%d exited=%d "
			"total=%d\n", clone_fd,
			atomic_load_explicit(&se->reading, memory_order_relaxed),
			fuse_session_received_inflight(se),
			atomic_load_explicit(&se->parked, memory_order_relaxed),
			atomic_load_explicit(&se->exited_workers,
					     memory_order_relaxed),
			atomic_load_explicit(&se->worker_total,
					     memory_order_relaxed));
		goto out_resume;
	}

	/* At the drained point every managed worker is parked (or exited) and
	 * none is reading or processing. */
	if (atomic_load_explicit(&se->reading, memory_order_relaxed) != 0 ||
	    fuse_session_received_inflight(se) != 0) {
		fprintf(stderr, "drained point not clean: reading=%d inflight=%d\n",
			atomic_load_explicit(&se->reading, memory_order_relaxed),
			fuse_session_received_inflight(se));
		goto out_resume;
	}
	{
		int parked = atomic_load_explicit(&se->parked,
						  memory_order_relaxed);
		int exited = atomic_load_explicit(&se->exited_workers,
						  memory_order_relaxed);
		int total = atomic_load_explicit(&se->worker_total,
						 memory_order_relaxed);
		if (parked + exited != total || total < 1) {
			fprintf(stderr,
				"parked accounting wrong: parked=%d exited=%d total=%d\n",
				parked, exited, total);
			goto out_resume;
		}
	}

	rc = 0;

out_resume:
	fuse_session_resume_receive(se);
	fuse_session_exit(se);
	pthread_join(loop_tid, NULL);
out_unmount:
	fuse_session_unmount(se);
out_destroy:
	fuse_session_destroy(se);
out_dir:
	rmdir(mountpoint);
	free(mountpoint);
	fuse_opt_free_args(&args);
	return rc;
}

int main(void)
{
	int rc;

	rc = run_pause(0);
	if (rc == 77) {
		printf("received_inflight test skipped (no mount capability)\n");
		return 77;
	}
	if (rc != 0)
		return rc;

	rc = run_pause(1);
	if (rc != 0)
		return rc;

	rc = run_wait_drained(0);
	if (rc != 0)
		return rc;

	rc = run_wait_drained(1);
	if (rc != 0)
		return rc;

	printf("pause/received_inflight/wait_drained test passed\n");
	return 0;
}
