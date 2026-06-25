# libfuse FUSE 热升级 controlled drain 设计文档

> 范围：本文只讨论 libfuse 层为了支持 FUSE daemon 热升级而新增的通用 controlled drain 能力，不包含任何具体文件系统的业务状态、flush 语义、进程握手、状态文件格式或部署方案。

## 1. 背景与目标

FUSE 用户态文件系统的挂载关系绑定在 `/dev/fuse` fd 对应的内核 FUSE connection 上，而不是绑定在某个用户态进程上。只要这条 connection 对应的内核对象仍然存活，内核就认为挂载仍然存在。

从这个事实出发，FUSE daemon 热升级的基础动作不是重新 mount，而是：

```text
停止老进程继续读取新请求
等待已经进入老进程的请求完成或超时
保存用户态必要状态
把 /dev/fuse fd 通过 SCM_RIGHTS 传给新进程
新进程恢复状态，并继续读取同一个 fd
```

本文只设计其中 libfuse 能提供的部分：

```text
把正在运行的 fuse_session_loop_mt()
带到一个可验证的 paused + drained 安全点。
```

也就是说，libfuse 要回答的问题是：

```text
老 daemon 什么时候可以被认为：
  1. 不会再从 /dev/fuse 读取新请求；
  2. 已经读取到用户态的请求都已处理完成；
  3. 所有 worker 都停在可交接状态。
```

libfuse 不负责完整热升级事务。以下能力仍由调用方实现：

- 通过 Unix domain socket + `SCM_RIGHTS` 传递 `/dev/fuse` fd。
- 保存和恢复文件系统私有状态。
- 业务层 flush、checkpoint、状态 dump/load。
- 新老进程 ACK/NACK 协议。
- 对端认证、upgrade id、超时策略。
- 新进程复用 fd 后如何恢复 INIT 协商状态。

## 2. 问题定义

### 2.1 stock `fuse_session_loop_mt()` 的退出语义不等于 handoff drain

libfuse 多线程 lowlevel loop 的核心路径是：

```text
worker loop
  -> fuse_session_receive_buf_internal()
  -> fuse_session_process_buf_internal()
```

请求被 `receive` 从 `/dev/fuse` 读出来后，已经离开内核 pending 队列，进入该 fd 对应的 processing 状态。此时请求字节已经在老进程内存里，新进程即使持有同一条 connection 的 fd，也不能重新读到这条请求。

因此热升级必须满足一个硬约束：

```text
凡是已经被老进程 read 到的请求，老进程必须 process/reply 完。
```

旧的 loop 中存在一个危险窗口：receive 成功后，如果 session 已经进入 exit 状态，worker 可能在 process 之前返回。对热升级来说，这等价于：

```text
请求已经被老进程认领
但没有 reply
新进程也读不到
```

这是热升级不能接受的。

### 2.2 `received_inflight == 0` 不是 handoff 屏障

只统计“已经 read 成功但尚未 process 完”的请求仍然不够。因为还有一类 worker 状态：

```text
worker 已经进入 read(/dev/fuse)，正在内核里阻塞等待下一个请求。
```

典型竞态：

```text
worker:
  检查 pause 标志为 false
  进入 read(/dev/fuse) 阻塞

control:
  设置 pause 标志
  看到 received_inflight == 0
  误判 drain 完成

application:
  发起新的文件系统操作

kernel:
  唤醒 blocked read

worker:
  read 返回新请求
  新请求进入老进程
```

所以 libfuse 需要显式区分：

```text
READING: worker 正在 receive/read
PROCESSING: worker 已经 read 到请求，正在 process/reply
PARKED: worker 已经看到 pause 标志，停在接收新请求之前
EXITED: worker 已退出 loop
```

安全点必须同时覆盖这些状态。

### 2.3 clone fd 让 drain 必须是 session 级

`fuse_session_loop_mt()` 可以启用 clone fd。额外 worker 会通过 `FUSE_DEV_IOC_CLONE` 克隆出独立 `/dev/fuse` fd。内核侧模型是：

```text
同一个 fuse_conn
  master fuse_dev  -> processing 队列 A
  clone fuse_dev 1 -> processing 队列 B
  clone fuse_dev 2 -> processing 队列 C
```

pending 队列属于 connection，processing 队列属于具体 `fuse_dev`。请求被哪个 fd read 走，就进入哪个 fd 的 processing 队列。

因此热升级 drain 不能只看主 fd。它必须覆盖同一个 `fuse_session` 下所有 worker，包括 master fd worker 和 clone fd worker。

该实现的设计选择是：所有 drain 相关标志和计数都放在 `struct fuse_session` 上，由所有 worker 共享。因此它的语义是 session-wide / connection-wide，而不是单 fd 局部 drain。

## 3. 设计目标与非目标

### 3.1 目标

1. 提供 API 暂停继续接收新请求。
2. 已经接收到用户态的请求必须继续 process/reply，不能因为 exit 被丢弃。
3. 提供可等待的 drain 条件，覆盖 reading、processing、parked、exited worker。
4. 支持 clone fd 场景，drain 统计覆盖 master worker 和 clone worker。
5. 支持失败回滚：drain 失败后可以 resume，让老进程继续服务。
6. 不调用新 API 时，保持原有 libfuse 行为。

### 3.2 非目标

1. 不提供 fd 传递 API。
2. 不提供新老进程握手协议。
3. 不保存或恢复文件系统私有状态。
4. 不提供业务 flush/checkpoint 语义。
5. 不提供 `FUSE_INIT` restore/init-without-reply API。
6. 不把 receive 改造成 poll/eventfd 可中断模型。

## 4. Public API 设计

本特性新增 4 个 public API，声明在 `include/fuse_lowlevel.h`，导出在 `lib/fuse_versionscript` 的 `FUSE_3.18.2` 节点。

```c
void fuse_session_pause_receive(struct fuse_session *se);
void fuse_session_resume_receive(struct fuse_session *se);
int  fuse_session_received_inflight(struct fuse_session *se);
int  fuse_session_wait_drained(struct fuse_session *se, int timeout_ms);
```

### 4.1 `fuse_session_pause_receive`

语义：

```text
设置 recv_paused。
worker 回到 loop 顶部后，在下一次 receive/read 之前 park。
```

性质：

- 幂等。
- session 级生效，覆盖 master fd 和 clone fd worker。
- 不会丢弃已经 receive 成功的请求。

限制：

```text
它不会唤醒已经阻塞在 read(/dev/fuse) 里的 worker。
```

因此它不能单独作为 handoff 屏障。调用方必须继续使用 wakeup 请求把 blocked read 顶出来，并用 `fuse_session_wait_drained()` 验证安全点。

### 4.2 `fuse_session_resume_receive`

语义：

```text
清除 recv_paused，唤醒 parked worker，使其继续 receive/read。
```

使用场景：

- drain 超时回滚。
- 上层 flush/dump 失败回滚。
- 新进程拒绝接管时恢复老进程服务。

### 4.3 `fuse_session_received_inflight`

返回：

```text
received_inflight 当前值。
```

它表示已经 receive 成功、尚未完成 `fuse_session_process_buf_internal()` 的请求数。

注意：

```text
received_inflight == 0 只是必要条件，不是 handoff 充分条件。
```

它不包含 blocked read worker。

### 4.4 `fuse_session_wait_drained`

语义：

```text
等待当前 session 的 worker pool 进入 drained 状态。
```

drained 条件：

```text
reading == 0
received_inflight == 0
parked + exited_workers == worker_total
```

返回值：

```text
0        成功进入 drained 状态
-1       超时
-EINVAL  timeout_ms < 0，或 session 未 pause
其他负 errno  pthread_cond_timedwait 等底层错误
```

调用约束：

- 必须先调用 `fuse_session_pause_receive()`。
- 只在正在运行的 `fuse_session_loop_mt()` 编排路径中有意义。
- `wait_drained()` 的 handoff 语义只属于当前正在运行的 `fuse_session_loop_mt()`。loop 未启动或已经退出时，即使计数看起来满足 drained 条件，也不能作为 handoff 依据。

timeout 使用 `se->drain_clock`。该实现优先初始化 `CLOCK_MONOTONIC` condvar；如果失败，退回默认 `CLOCK_REALTIME`，并保证 timedwait deadline 使用同一 clock。

### 4.5 `fuse_session_reset` 行为扩展

`fuse_session_reset()` 是 libfuse 已有 public API。本特性没有修改它的函数签名，但扩展了它的内部行为：

```text
mt_exited = false
error = 0
recv_paused = false
received_inflight = 0
reading = 0
parked = 0
exited_workers = 0
worker_total = 0
```

这样可以避免一次 drain/pause 之后，如果调用方 reset session，旧的 paused 标志或计数残留影响后续 loop。否则可能出现：

```text
上一次 loop 已 pause
调用 fuse_session_reset()
重新进入 loop
新 worker 立刻看到 recv_paused=true 并全部 park
```

调用契约：

- 只能在 loop 已退出、没有活跃 worker 时调用。
- 不能在运行中的 drain/loop 上调用；否则清零计数会破坏正在运行 worker 的统计。
- 这只是清理 drain 状态，不表示 `fuse_session_reset()` 提供完整的 `fuse_session_loop_mt()` lifecycle 重建能力。
- `wait_drained()` 的可靠语义仍只属于当前正在运行的 live loop。

该行为有回归测试覆盖：`test/test_reset_clears_drain.c`。

## 5. 内部状态模型

`struct fuse_session` 在 `lib/fuse_i.h` 中追加 drain 状态字段：

```c
_Atomic bool recv_paused;
_Atomic int  received_inflight;
_Atomic int  worker_total;
_Atomic int  reading;
_Atomic int  parked;
_Atomic int  exited_workers;
pthread_mutex_t drain_lock;
pthread_cond_t  drain_cond;
clockid_t       drain_clock;
```

含义：

| 字段 | 含义 |
|---|---|
| `recv_paused` | 是否暂停 worker 发起下一次 receive |
| `received_inflight` | 瞬时值：已 receive 成功、尚未 process 完的请求数 |
| `worker_total` | 累计值：当前 loop generation 内创建成功并纳入管理的 worker 总数 |
| `reading` | 瞬时值：正处于 receive/read 窗口的 worker 数 |
| `parked` | 瞬时值：已经停在 pause 点等待 resume/exit 的 worker 数 |
| `exited_workers` | 累计值：当前 loop generation 内已经离开 worker loop 的 worker 数 |
| `drain_lock` / `drain_cond` | `wait_drained` 和 parked worker 的同步对象 |
| `drain_clock` | `drain_cond` timedwait 使用的 clock |

这些字段全部属于 libfuse 内部结构。外部调用方只能通过新增 API 访问，不依赖字段布局。

## 6. Worker Loop 改造

### 6.1 pause 检查点

worker 每轮 receive 前检查 `recv_paused`：

```text
while !session_exited:
  if recv_paused:
    parked++
    wait drain_cond until resume or exit
    parked--
    continue

  receive/read
  process
```

这个位置保证了 pause 的语义是：

```text
不再开始下一次 receive。
```

它不声称能中断已经开始的 receive。

### 6.2 reading 计数

进入 `fuse_session_receive_buf_internal()` 前：

```text
reading++
```

返回后：

```text
reading--
```

每次 reading 状态变化都会唤醒 `wait_drained()` 重新检查条件。

### 6.3 received_inflight 计数

receive 成功后：

```text
received_inflight++
```

`fuse_session_process_buf_internal()` 返回后：

```text
received_inflight--
```

这保证了已 read 请求一定被计入 processing 窗口。

实现上，`received_inflight++` 不 broadcast `drain_cond`，只有 `received_inflight--` 会唤醒 waiter。原因是自增只会让状态远离 drained 条件，不可能让 `wait_drained()` 变成可满足；自减才可能让 `received_inflight == 0` 成立。这个选择是有意避免无意义唤醒，不影响正确性。

### 6.4 删除 receive 后丢请求路径

本特性的关键行为变化是：receive 成功后，不再因为 session 已经 exit 而在 process 前直接返回。

新的约束是：

```text
只要 receive 成功，这个请求就必须走完 process_buf。
```

exit 只能阻止后续继续 receive，不能丢弃已经拿到的请求。

### 6.5 worker_total 预计数

`worker_total` 在 `fuse_loop_start_thread()` 中预先增加。线程创建失败时再回滚。

这样可以避免 create-vs-pause race：

```text
worker 已创建但尚未进入线程函数
control pause + wait_drained
如果 worker_total 未计入该 worker，wait_drained 可能提前成功
```

预计数的结果是偏保守：只要线程创建成功，即使还没运行，也会阻止 `wait_drained()` 过早返回。

还有一个容易误解的瞬态：worker 在 `reading--` 之后，到下一轮 loop 顶部 `parked++` 之前，短时间内既不计入 `reading`，也还没计入 `parked/exited`。这仍然不会造成假阳性，因为该 worker 已经包含在 `worker_total` 里，而此时：

```text
parked + exited_workers < worker_total
```

drained 谓词仍然为 false，`wait_drained()` 会继续等待，直到该 worker park、exit，或再次进入 read/process 并更新对应计数。

### 6.6 pause 后禁止 worker pool 扩容

原 `fuse_session_loop_mt()` 会按负载扩容 worker。本特性在 `recv_paused` 为 true 后禁止继续启动新 worker。

原因：

```text
pause 之后 worker_total 应该稳定，否则 drained 目标会继续变化。
```

### 6.7 parked worker cancellation safety

worker 默认禁用 pthread cancellation，只在 receive/read 窗口临时打开 cancellation。

原因是 parked worker 停在 `pthread_cond_wait()`。如果它在这里被 cancel，可能出现：

```text
parked++ 已经执行
parked-- 没执行
exited_workers++ 没执行
drain_lock 状态被破坏
```

该实现避免 parked worker 在 cond_wait 中被 cancel。普通 shutdown 仍然可以通过 receive/read 窗口 cancellation 唤醒阻塞在 read 的 worker。

三个窗口的 cancellation 语义不同：

| worker 所在窗口 | 是否允许 cancel | 原因 |
|---|---|---|
| 阻塞在 receive/read | 允许 | worker 手里还没有用户态请求；普通 shutdown 需要用 cancel 打断阻塞 read，否则 loop 可能无法退出 |
| `process_buf` 处理请求 | 禁止 | 请求已经被 read 到用户态，cancel 会造成已读未 reply 请求丢失 |
| paused 后 parked 在 `pthread_cond_wait()` | 禁止 | 已经修改 `parked` 计数，cancel 可能跳过 `parked--` / `exited_workers++`，并破坏 `drain_lock` 状态 |

## 7. Blocked Read Wakeup

`fuse_session_pause_receive()` 不会唤醒 blocked `read(/dev/fuse)`。这是当前设计的核心边界。

调用方需要主动制造一次 FUSE 往返，让 blocked read 正常返回。常见选择是：

```text
statfs(mountpoint)
```

时序：

```text
worker:
  read(/dev/fuse) 阻塞

control:
  fuse_session_pause_receive()
  发起 statfs(mountpoint)

kernel:
  生成 FUSE_STATFS 请求
  唤醒某个 blocked read

worker:
  read 返回
  process/reply statfs 或其他到达的请求
  回到 loop 顶部
  看到 recv_paused
  parked++
```

注意：

- wakeup 请求应由独立线程或异步任务发起，避免控制线程自己卡死。
- wakeup 请求本身要有超时或可放弃。
- drain 期间老进程读到的不一定是 statfs，也可能是真实业务请求。
- 这不是错误，只要请求被完整 process/reply，并且状态 dump 发生在 `wait_drained()` 成功之后。

本特性没有采用 poll/eventfd 可中断 receive，因此不会给正常每个请求增加一次 poll 系统调用。

## 8. 推荐调用时序

下面是 libfuse 视角的通用热升级时序。它是推荐的 daemon 编排方式，不是 libfuse API 强制流程；libfuse 只保证 `pause_receive` / `wait_drained` / `resume_receive` 的语义。具体 fd 传递、控制消息名称和 ACK/NACK 协议由调用方定义。

```text
Old daemon                                      New daemon
----------                                      ----------
正常运行 fuse_session_loop_mt

                                                启动
                                                建立控制连接

通过 SCM_RIGHTS 发送 /dev/fuse fd 副本  ---->  持有 fd，但不 read

fuse_session_pause_receive(se)

循环:
  异步触发 statfs(mountpoint)
  rc = fuse_session_wait_drained(se, short_timeout)
  if rc == 0:
      break
  if rc != -1:
      resume + abort handoff

drain 成功

上层 flush/checkpoint/dump

                                                复用 /dev/fd/N
                                                恢复用户态状态
                                                准备开始服务

收到上层接管确认
fuse_session_exit(se)
old loop 返回
old 退出
                                                开始 fuse_session_loop_mt
```

失败回滚：

```text
如果 drain 超时或上层状态准备失败:
  fuse_session_resume_receive(se)
  通知 new 关闭 fd 副本
  old 继续服务
```

关键约束：

- new 可以先持有 fd，但在 old drain 完成前不能读。
- `pause_receive()` 之后必须用 `wait_drained()`，不能只看 `received_inflight`。
- `fuse_session_exit()` 应发生在 drain 成功和上层确认之后。
- 不要用 `fuse_session_exit()` 代替 drain。

## 9. 失败处理语义

### 9.1 drain 超时

`fuse_session_wait_drained()` 返回 `-1` 表示超时。可能原因：

- 有 worker 一直在 read，wakeup 没有把它顶出来。
- 有请求正在 process，业务回调长时间不返回。
- 有 worker 尚未 parked/exited。

推荐处理：

```text
fuse_session_resume_receive(se)
关闭新进程 fd 副本
本次热升级失败
老进程继续服务
```

不要在 drain 未完成时退出老进程。

### 9.2 wait_drained 返回其他错误

`-EINVAL` 通常表示调用顺序错误，例如未 pause 或 timeout 为负。其他负 errno 表示底层 wait 错误。调用方应视为 drain 失败，执行 resume 回滚。

### 9.3 old 不健康

fd 副本只能保活内核 connection，不能恢复 old 已经 read 到用户态但尚未 reply 的请求。

如果 old 已经卡死或崩溃在某个 processing 请求上，新进程即使持有 fd，也不能重新读到这条请求。因此 controlled drain 的前提是：

```text
old 进程仍健康，能够完成 drain。
```

old 不健康时，应把 live handoff 判失败，由外部恢复策略处理。

## 10. ABI 与兼容性

本特性的 ABI 策略：

1. 新增 public symbol，不修改已有 symbol。
2. `struct fuse_session` 是 libfuse 内部结构，新增字段追加在内部结构中。
3. 不调用新 API 时，`recv_paused` 默认 false，worker loop 行为应保持原有语义。
4. `fuse_session_reset()` 的签名和 symbol 不变；行为扩展为一并清空 controlled drain 状态。对不使用 drain 的老调用方无感知，因为这些计数本来就是内部状态且默认值为 0。
5. 测试 hook 只在 `FUSE_TEST_DRAIN_HOOKS` test-only 静态库中启用，不进入安装库，不是 public ABI。

新增 symbol：

```text
FUSE_3.18.2:
  fuse_session_pause_receive
  fuse_session_resume_receive
  fuse_session_received_inflight
  fuse_session_wait_drained
```

## 11. 总结

libfuse controlled drain 提供的是一个热升级基础同步原语：

```text
pause_receive + wakeup blocked read + wait_drained
```

其中 `wait_drained()` 的成功条件是：

```text
reading == 0
received_inflight == 0
parked + exited_workers == worker_total
```

这组条件把过去模糊的“老进程差不多不读了”变成一个可等待、可测试、可回滚的 libfuse 状态。

完整热升级仍要由具体 daemon 在其上实现 fd handoff、状态持久化、flush gate、ACK/NACK、认证和失败回滚。libfuse 只负责把 FUSE 请求处理面带到一个可证明的静止点。
