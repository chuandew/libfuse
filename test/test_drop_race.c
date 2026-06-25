/*
 * FUSE: Filesystem in Userspace
 *
 * Deterministic regression test for the "drop a received request on exit"
 * race in fuse_do_work().
 *
 * Before the fix, a worker that had successfully received a request but had
 * not yet processed it would hit `if (fuse_session_exited(se)) return NULL;`
 * and silently drop the request, leaving the issuing syscall hung forever.
 *
 * This test compiles libfuse with -DFUSE_TEST_DRAIN_HOOKS, which fires
 * se->test_after_receive_hook in exactly that window (after receive, before
 * process). The hook here calls fuse_session_exit(se), forcing the race every
 * time. A client thread issues a getattr (stat) over the mount; the test
 * passes iff that stat completes (the request was processed and replied)
 * rather than hanging.
 *
 * Red/green anchor: re-introducing the early "if (exited) return NULL" drop
 * makes the stat hang and this test time out.
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
#include <sys/types.h>
#include <unistd.h>

static atomic_int hook_fired;

static void after_receive_hook(struct fuse_session *se)
{
	/* Skip the kernel INIT handshake (got_init not yet set when its own
	 * request is in this window); fire only on the first real FS request
	 * so the raced request is the client's getattr, not INIT. */
	if (!se->got_init)
		return;

	/* Fire only once: simulate the session being told to exit right after
	 * a worker received a request but before it processed it. With the fix
	 * that request is still processed; received_inflight must reflect this
	 * single in-flight request. */
	int expected = 0;
	if (!atomic_compare_exchange_strong(&hook_fired, &expected, 1))
		return;

	if (fuse_session_received_inflight(se) != 1) {
		fprintf(stderr, "received_inflight expected 1, got %d\n",
			fuse_session_received_inflight(se));
		_exit(2);
	}

	fuse_session_exit(se);
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

static atomic_int stat_done;
static atomic_int stat_ok;

/* Client thread: stat the mount root, which drives one getattr request that
 * races with the hook-triggered exit. The stat must return. */
static void *stat_thread(void *arg)
{
	char *mountpoint = arg;
	struct stat st;

	if (stat(mountpoint, &st) == 0)
		atomic_store(&stat_ok, 1);
	atomic_store(&stat_done, 1);
	return NULL;
}

static int run_once(void)
{
	struct fuse_args args = FUSE_ARGS_INIT(0, NULL);
	struct fuse_session *se;
	struct fuse_loop_config *loop_config;
	char *mountpoint;
	pthread_t client;
	int i, rc = 1;

	atomic_store(&hook_fired, 0);
	atomic_store(&stat_done, 0);
	atomic_store(&stat_ok, 0);

	if (fuse_opt_add_arg(&args, "test_drop_race"))
		return 1;

	mountpoint = strdup("/tmp/fuse_droprace_XXXXXX");
	if (!mountpoint || !mkdtemp(mountpoint)) {
		fprintf(stderr, "Failed to create temp dir\n");
		free(mountpoint);
		fuse_opt_free_args(&args);
		return 1;
	}

	se = fuse_session_new(&args, &test_ll_ops, sizeof(test_ll_ops), NULL);
	if (!se) {
		fprintf(stderr, "Failed to create session\n");
		goto out_dir;
	}

	se->test_after_receive_hook = after_receive_hook;

	if (fuse_session_mount(se, mountpoint)) {
		fprintf(stderr, "Failed to mount (need setuid fusermount3)\n");
		rc = 77;
		goto out_destroy;
	}

	/* Issue the client stat from a thread; the loop runs below on the main
	 * thread and tears down once the hook fires. */
	if (pthread_create(&client, NULL, stat_thread, mountpoint))
		goto out_unmount;

	loop_config = fuse_loop_cfg_create();
	fuse_loop_cfg_set_clone_fd(loop_config, 0);
	fuse_loop_cfg_set_max_threads(loop_config, 2);

	/* The hook calls fuse_session_exit() right after the first receive;
	 * the loop must still process+reply that request, then return. */
	fuse_session_loop_mt_312(se, loop_config);
	fuse_loop_cfg_destroy(loop_config);

	/* Bounded wait for the client stat to return (it must, if not dropped). */
	for (i = 0; i < 500 && !atomic_load(&stat_done); i++)
		usleep(10 * 1000);

	pthread_join(client, NULL);

	if (!atomic_load(&hook_fired)) {
		fprintf(stderr, "hook never fired (no request received)\n");
		rc = 1;
	} else if (!atomic_load(&stat_done)) {
		fprintf(stderr, "client stat did not return (request dropped)\n");
		rc = 1;
	} else if (!atomic_load(&stat_ok)) {
		fprintf(stderr, "client stat returned an error\n");
		rc = 1;
	} else if (fuse_session_received_inflight(se) != 0) {
		fprintf(stderr, "received_inflight leaked: %d\n",
			fuse_session_received_inflight(se));
		rc = 1;
	} else {
		rc = 0;
	}

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
	int rc = run_once();

	if (rc == 77) {
		printf("drop-race test skipped (no mount capability)\n");
		return 77;
	}
	if (rc == 0)
		printf("drop-race test passed\n");
	return rc;
}
