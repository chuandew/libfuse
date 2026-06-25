/*
 * FUSE: Filesystem in Userspace
 *
 * Deterministic regression test for the worker_total create-vs-pause race
 * (reviewer "blocker-1").
 *
 * The bug: worker_total++ used to run in the worker thread body, so there was
 * a window between fuse_loop_start_thread() succeeding (pthread_create
 * returned) and the new worker reaching that increment. If a pause + drain
 * landed in that window, wait_drained() could observe
 *     parked + exited_workers == worker_total
 * and WRONGLY report drained, while the not-yet-counted worker was about to
 * wake, receive a request and process it AFTER the orchestrator had already
 * dumped state / handed off the fd -- breaking the "dump happens after drain"
 * guarantee. (A correctness/consistency hazard, not a hang.)
 *
 * The fix moves worker_total++ into fuse_loop_start_thread() under mt_lock,
 * next to numworker++, so a successfully created worker is counted
 * synchronously at create time.
 *
 * How this test pins the window deterministically (needs -DFUSE_TEST_DRAIN_HOOKS
 * so the test-only hooks are compiled in):
 *   - The FS lookup blocks on a gate. The initial single worker receives the
 *     first lookup, sets numavail==0, and burst-spawns a second worker W2.
 *   - W2 hits se->test_worker_entry_hook at the very top of its thread body
 *     and parks there on a barrier the test controls -- i.e. W2 exists and
 *     fuse_loop_start_thread() has returned, but W2 has NOT run its loop:
 *     exactly the "created but not yet running" state.
 *   - The test then pauses receiving and releases the held lookup so the first worker
 *     finishes it and parks at the drain pause boundary.
 *   - Assertion: wait_drained() must NOT report drained, because W2 is counted
 *     in worker_total (pre-counted at create time) yet is neither parked nor
 *     exited. The gate is then released and a real drain is confirmed to
 *     complete, so the fix does not merely wedge the drain.
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

/* Lookup gate: holds the first request in flight so the pool burst-spawns W2. */
static pthread_mutex_t lookup_gate_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t lookup_gate_cond = PTHREAD_COND_INITIALIZER;
static int lookup_gate_open;
static atomic_int lookups_entered;

/* Entry gate: holds the spawned worker W2 at the top of its thread body. */
static pthread_mutex_t entry_gate_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t entry_gate_cond = PTHREAD_COND_INITIALIZER;
static int entry_gate_open;
static atomic_int workers_entered;   /* how many workers passed the entry hook */

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

static void test_ll_lookup(fuse_req_t req, fuse_ino_t parent, const char *name)
{
	(void)parent;
	(void)name;

	atomic_fetch_add(&lookups_entered, 1);

	pthread_mutex_lock(&lookup_gate_lock);
	while (!lookup_gate_open)
		pthread_cond_wait(&lookup_gate_cond, &lookup_gate_lock);
	pthread_mutex_unlock(&lookup_gate_lock);

	fuse_reply_err(req, ENOENT);
}

static const struct fuse_lowlevel_ops test_ll_ops = {
	.lookup = test_ll_lookup,
	.getattr = test_ll_getattr,
};

/* Entry hook: the FIRST worker to enter (the initial pool worker, created
 * before any hook was installed... actually it is installed before the loop is
 * started, so the initial worker also hits this) is let through; every
 * subsequent spawned worker is held until the test opens the entry gate. We
 * only want to trap the burst-spawned W2, so we let exactly the first caller
 * pass and park the rest. */
static void worker_entry_hook(struct fuse_session *se)
{
	(void)se;
	int idx = atomic_fetch_add(&workers_entered, 1);

	if (idx == 0)
		return; /* initial pool worker: let it run so it can serve the
			 * first lookup and burst-spawn W2 */

	/* W2 (and any further spawns): park at the top of the thread body. */
	pthread_mutex_lock(&entry_gate_lock);
	while (!entry_gate_open)
		pthread_cond_wait(&entry_gate_cond, &entry_gate_lock);
	pthread_mutex_unlock(&entry_gate_lock);
}

static void open_gate(pthread_mutex_t *l, pthread_cond_t *c, int *flag)
{
	pthread_mutex_lock(l);
	*flag = 1;
	pthread_cond_broadcast(c);
	pthread_mutex_unlock(l);
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

static void *stat_thread(void *arg)
{
	char *path = arg;
	struct stat st;

	stat(path, &st); /* expected ENOENT; only needs to return */
	return NULL;
}

static int wait_atomic(atomic_int *v, int n, int max_ms)
{
	int i;

	for (i = 0; i < max_ms / 10 && atomic_load(v) < n; i++)
		usleep(10 * 1000);
	return atomic_load(v) >= n;
}

static int run_once(int clone_fd)
{
	struct fuse_args args = FUSE_ARGS_INIT(0, NULL);
	struct fuse_session *se;
	char *mountpoint;
	pthread_t loop_tid, c0;
	char p0[256];
	struct loop_arg la;
	int i, rc = 1, drained;

	lookup_gate_open = 0;
	entry_gate_open = 0;
	atomic_store(&lookups_entered, 0);
	atomic_store(&workers_entered, 0);

	if (fuse_opt_add_arg(&args, "test_drain_spawn_race"))
		return 1;

	mountpoint = strdup("/tmp/fuse_spawnrace_XXXXXX");
	if (!mountpoint || !mkdtemp(mountpoint)) {
		free(mountpoint);
		fuse_opt_free_args(&args);
		return 1;
	}

	se = fuse_session_new(&args, &test_ll_ops, sizeof(test_ll_ops), NULL);
	if (!se)
		goto out_dir;

	se->test_worker_entry_hook = worker_entry_hook;

	if (fuse_session_mount(se, mountpoint)) {
		fprintf(stderr, "mount failed (need setuid fusermount3)\n");
		rc = 77;
		goto out_destroy;
	}

	la.se = se;
	la.clone_fd = clone_fd;
	if (pthread_create(&loop_tid, NULL, loop_thread, &la))
		goto out_unmount;

	snprintf(p0, sizeof(p0), "%s/name0", mountpoint);

	/* Drive the first lookup; it is held in the gate, the initial worker
	 * has numavail==0 and burst-spawns W2. */
	if (pthread_create(&c0, NULL, stat_thread, p0))
		goto out_release_all;

	if (!wait_atomic(&lookups_entered, 1, 5000)) {
		fprintf(stderr, "first lookup never reached handler\n");
		goto out_release_all_join;
	}

	/* Wait for W2 to be spawned and reach the entry hook (workers_entered
	 * becomes 2: idx0 = initial worker passed through, idx1 = W2 parked). */
	if (!wait_atomic(&workers_entered, 2, 5000)) {
		fprintf(stderr, "spawned worker W2 never reached entry hook "
			"(workers_entered=%d) -- pool did not grow\n",
			atomic_load(&workers_entered));
		goto out_release_all_join;
	}

	/* W2 now exists (fuse_loop_start_thread returned) but is parked at the
	 * entry hook, having run no loop body: the create-vs-pause window.
	 * Pause receiving, then release the held lookup so the initial worker finishes
	 * it and parks at the drain pause boundary.
	 *
	 * No statfs is driven here: recv_paused stops the initial worker at the
	 * top of its loop (before receive), so once it replies the held lookup
	 * it parks on its own without needing a fresh request -- driving statfs
	 * while the only live worker is about to park would just block forever. */
	fuse_session_pause_receive(se);
	open_gate(&lookup_gate_lock, &lookup_gate_cond, &lookup_gate_open);
	pthread_join(c0, NULL);

	/* Wait for the initial worker to finish the in-flight request and park:
	 * received_inflight back to 0, reading 0, and at least one worker parked.
	 * W2 is still stuck in the entry hook (not parked, not exited). */
	for (i = 0; i < 250; i++) {
		if (atomic_load_explicit(&se->received_inflight,
					 memory_order_relaxed) == 0 &&
		    atomic_load_explicit(&se->reading,
					 memory_order_relaxed) == 0 &&
		    atomic_load_explicit(&se->parked, memory_order_relaxed) >= 1)
			break;
		usleep(10 * 1000);
	}

	/*
	 * THE ANCHOR: W2 is counted in worker_total (post-fix) but is neither
	 * parked nor exited (it is stuck in the entry hook). So
	 *     parked + exited_workers != worker_total
	 * and wait_drained() MUST NOT report drained. With the pre-fix bug W2
	 * was uncounted, the equality held, and this returned 0 (RED).
	 */
	drained = fuse_session_wait_drained(se, 500);
	if (drained == 0) {
		fprintf(stderr,
			"REGRESSION: wait_drained reported drained while a "
			"spawned worker had not run (clone_fd=%d): "
			"reading=%d received_inflight=%d parked=%d exited=%d "
			"total=%d\n", clone_fd,
			atomic_load_explicit(&se->reading, memory_order_relaxed),
			atomic_load_explicit(&se->received_inflight,
					     memory_order_relaxed),
			atomic_load_explicit(&se->parked, memory_order_relaxed),
			atomic_load_explicit(&se->exited_workers,
					     memory_order_relaxed),
			atomic_load_explicit(&se->worker_total,
					     memory_order_relaxed));
		goto out_release_all_loop;
	}

	/*
	 * Sanity: once W2 is released it runs its body, sees recv_paused at the
	 * top of its loop and parks at the drain pause boundary too. A real drain
	 * must then complete -- proving the fix does not merely wedge
	 * wait_drained() forever. No statfs needed: both workers reach the park
	 * point on their own (neither has a request to serve).
	 */
	open_gate(&entry_gate_lock, &entry_gate_cond, &entry_gate_open);
	drained = fuse_session_wait_drained(se, 2000);
	if (drained != 0) {
		fprintf(stderr,
			"drain never completed after releasing W2 (clone_fd=%d): "
			"parked=%d exited=%d total=%d\n", clone_fd,
			atomic_load_explicit(&se->parked, memory_order_relaxed),
			atomic_load_explicit(&se->exited_workers,
					     memory_order_relaxed),
			atomic_load_explicit(&se->worker_total,
					     memory_order_relaxed));
		goto out_resume;
	}

	rc = 0;

out_resume:
	fuse_session_resume_receive(se);
	fuse_session_exit(se);
	pthread_join(loop_tid, NULL);
	goto out_unmount;

out_release_all_loop:
	/* error after the loop thread is running and gates may be closed */
	open_gate(&lookup_gate_lock, &lookup_gate_cond, &lookup_gate_open);
	open_gate(&entry_gate_lock, &entry_gate_cond, &entry_gate_open);
	fuse_session_resume_receive(se);
	fuse_session_exit(se);
	pthread_join(loop_tid, NULL);
	goto out_unmount;

out_release_all_join:
	open_gate(&lookup_gate_lock, &lookup_gate_cond, &lookup_gate_open);
	open_gate(&entry_gate_lock, &entry_gate_cond, &entry_gate_open);
	pthread_join(c0, NULL);
	fuse_session_exit(se);
	pthread_join(loop_tid, NULL);
	goto out_unmount;

out_release_all:
	open_gate(&lookup_gate_lock, &lookup_gate_cond, &lookup_gate_open);
	open_gate(&entry_gate_lock, &entry_gate_cond, &entry_gate_open);
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

	rc = run_once(0);
	if (rc == 77) {
		printf("drain-spawn-race test skipped (no mount capability)\n");
		return 77;
	}
	if (rc != 0)
		return rc;

	rc = run_once(1);
	if (rc == 77)
		return 77;
	if (rc != 0)
		return rc;

	printf("drain-spawn-race regression test passed\n");
	return 0;
}
