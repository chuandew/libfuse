/*
 * FUSE: Filesystem in Userspace
 *
 * End-to-end test for the core hot-upgrade drain handoff path on a real
 * /dev/fuse mount:
 *
 *   pause receiving -> wake the worker blocked in read() with a statfs(2) round-trip
 *           -> fuse_session_wait_drained() returns 0
 *           -> (NO resume) fuse_session_exit()
 *           -> the session loop returns cleanly (join completes, counters
 *              self-consistent, no hang).
 *
 * This is the production sequence the orchestrator runs on the OLD process up
 * to the point it would dump state and hand off the fd. It exercises the real
 * statfs-wakeup mechanism (a worker parked in read() is only popped out by an
 * actual FUSE round-trip), not a synthetic injection.
 *
 * Determinism / no-hang: every blocking join uses pthread_timedjoin_np with a
 * watchdog; on expiry the test reports failure instead of wedging. The statfs
 * round-trip is driven from a short-lived thread (it must not block the control
 * flow if, in a regression, no worker services it) and also watchdog-joined.
 *
 * Runs clone_fd=0 (required) and clone_fd=1 (clone-worker coverage). Needs a
 * working mount (setuid fusermount3); exits 77 to skip otherwise.
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
#include <sys/statvfs.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

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

/* A real statfs reply so the wakeup round-trip completes cleanly. */
static void test_ll_statfs(fuse_req_t req, fuse_ino_t ino)
{
	(void)ino;
	struct statvfs st;

	memset(&st, 0, sizeof(st));
	st.f_bsize = 4096;
	st.f_frsize = 4096;
	st.f_namemax = 255;
	fuse_reply_statfs(req, &st);
}

static const struct fuse_lowlevel_ops test_ll_ops = {
	.getattr = test_ll_getattr,
	.statfs = test_ll_statfs,
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

/* Short-lived thread issuing one statfs round-trip against the mount, to pop a
 * worker out of a blocking read(). Run on its own thread so a regression where
 * nothing services it cannot block the test's control flow. */
static void *statfs_thread(void *arg)
{
	char *mountpoint = arg;
	struct statvfs vfs;

	statvfs(mountpoint, &vfs);
	return NULL;
}

static int timedjoin(pthread_t tid, int ms)
{
	struct timespec deadline;

	clock_gettime(CLOCK_REALTIME, &deadline);
	deadline.tv_sec += ms / 1000;
	deadline.tv_nsec += (long)(ms % 1000) * 1000000L;
	if (deadline.tv_nsec >= 1000000000L) {
		deadline.tv_sec += 1;
		deadline.tv_nsec -= 1000000000L;
	}
	return pthread_timedjoin_np(tid, NULL, &deadline);
}

static int run_once(int clone_fd)
{
	struct fuse_args args = FUSE_ARGS_INIT(0, NULL);
	struct fuse_session *se;
	char *mountpoint;
	pthread_t loop_tid;
	struct loop_arg la;
	int i, rc = 1, drained = -1;

	if (fuse_opt_add_arg(&args, "test_drain_handoff"))
		return 1;

	mountpoint = strdup("/tmp/fuse_handoff_XXXXXX");
	if (!mountpoint || !mkdtemp(mountpoint)) {
		free(mountpoint);
		fuse_opt_free_args(&args);
		return 1;
	}

	se = fuse_session_new(&args, &test_ll_ops, sizeof(test_ll_ops), NULL);
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

	/* Let the loop spin up and finish the kernel INIT handshake, then let
	 * workers settle into a blocking read(). A getattr round-trip ensures
	 * the session is fully initialised. */
	{
		struct stat stx;
		stat(mountpoint, &stx);
	}
	usleep(100 * 1000);

	/*
	 * Production drain sequence on the OLD process:
	 * 1) pause receiving: workers stop taking new work at the next loop top, but a
	 *    worker already blocked in read() is NOT woken by this.
	 * 2) drive statfs round-trips to pop blocked readers out so they return
	 *    to the loop top and park; poll wait_drained() until it reports the
	 *    drained safe point.
	 */
	fuse_session_pause_receive(se);

	for (i = 0; i < 50; i++) {
		pthread_t st_tid;

		if (pthread_create(&st_tid, NULL, statfs_thread, mountpoint) == 0) {
			/* The statfs must come back promptly while a worker is
			 * still serving; watchdog-join it so a regression can't
			 * block us. */
			if (timedjoin(st_tid, 1000) == ETIMEDOUT) {
				/* Leave it; a worker may have already parked. */
				pthread_detach(st_tid);
			}
		}
		drained = fuse_session_wait_drained(se, 100);
		if (drained == 0)
			break;
	}

	if (drained != 0) {
		fprintf(stderr,
			"wait_drained never reached the drained point "
			"(clone_fd=%d): reading=%d received_inflight=%d "
			"parked=%d exited=%d total=%d\n", clone_fd,
			atomic_load_explicit(&se->reading, memory_order_relaxed),
			fuse_session_received_inflight(se),
			atomic_load_explicit(&se->parked, memory_order_relaxed),
			atomic_load_explicit(&se->exited_workers,
					     memory_order_relaxed),
			atomic_load_explicit(&se->worker_total,
					     memory_order_relaxed));
		fuse_session_exit(se);
		if (timedjoin(loop_tid, 5000) == ETIMEDOUT)
			goto out_unmount_nowait;
		goto out_unmount;
	}

	/*
	 * Drained: dump/handoff would happen here. Crucially we do NOT
	 * resume -- the real handoff exits the old process from the drained,
	 * paused state. fuse_session_exit() must unwind the loop cleanly: the
	 * parked workers are woken by exit's drain_cond broadcast and exit
	 * without being force-cancelled mid-flight.
	 */
	fuse_session_exit(se);

	if (timedjoin(loop_tid, 5000) == ETIMEDOUT) {
		fprintf(stderr,
			"REGRESSION: session loop did not return after "
			"drained-then-exit (clone_fd=%d) -- handoff path wedged "
			"(parked=%d exited=%d total=%d)\n", clone_fd,
			atomic_load_explicit(&se->parked, memory_order_relaxed),
			atomic_load_explicit(&se->exited_workers,
					     memory_order_relaxed),
			atomic_load_explicit(&se->worker_total,
					     memory_order_relaxed));
		goto out_unmount_nowait;
	}

	/* Counters must be self-consistent after a clean teardown. */
	{
		int parked = atomic_load_explicit(&se->parked,
						  memory_order_relaxed);
		int exited = atomic_load_explicit(&se->exited_workers,
						  memory_order_relaxed);
		int total = atomic_load_explicit(&se->worker_total,
						 memory_order_relaxed);
		int reading = atomic_load_explicit(&se->reading,
						   memory_order_relaxed);
		if (parked != 0 || exited != total || total < 1 || reading != 0) {
			fprintf(stderr,
				"counter inconsistency after handoff teardown "
				"(clone_fd=%d): reading=%d parked=%d exited=%d "
				"total=%d\n", clone_fd, reading, parked, exited,
				total);
			goto out_unmount;
		}
	}

	rc = 0;
	goto out_unmount;

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
	/* Wedged loop thread intentionally not joined; unmount the kernel side
	 * and report failure without destroying the live session. */
	fuse_session_unmount(se);
	rmdir(mountpoint);
	free(mountpoint);
	fuse_opt_free_args(&args);
	return rc;
}

int main(void)
{
	int rc;

	rc = run_once(0);
	if (rc == 77) {
		printf("drain-handoff test skipped (no mount capability)\n");
		return 77;
	}
	if (rc != 0)
		return rc;

	rc = run_once(1);
	if (rc == 77)
		return 77;
	if (rc != 0)
		return rc;

	printf("drain-handoff end-to-end test passed\n");
	return 0;
}
