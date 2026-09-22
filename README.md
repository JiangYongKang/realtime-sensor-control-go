# realtime-sensor-control-go

本地可运行的实时传感器采样与安全控制通道。使用模拟设备（`internal/sensor`）
与可手动推进的时钟（`internal/clock`）产生完全确定、可复现的采样数据与
总线报文，使调度、协议兼容与故障降级在真实工程环境下得到一致且可解释的结论。

## 结构

| 包 | 职责 |
|---|---|
| `internal/clock` | 手动推进时钟，`Advance` 单调递增 |
| `internal/sensor` | 确定性模拟设备（正弦信号，同时刻同输出） |
| `internal/scheduler` | 多路周期/优先级采样调度 |
| `internal/protocol` | 带版本的总线报文编解码（TLV + CRC32） |
| `internal/safety` | 看门狗与安全降级状态机 |
| `internal/system` | 编排层：采样、控制输出、总线、状态快照 |
| `cmd/sensord` | 本地演示入口 |

## 调度与时间语义

- 外部通过 `System.Advance(t)` 单调推进时间；每次调用处理 `(上次时刻, t]`
  内所有到期采样，处理顺序按 `(计划时刻, 优先级, 通道ID)` 排序，完全确定。
- 到期时刻始终以 `start + k*Period` 的整数倍网格计算，逐周期推进，
  长时间运行**不产生累积漂移**（见 `TestNoCumulativeDrift`）。
- 时钟大步前进时，被跨过的每个周期都会各产生一条到期记录，
  **不会被静默吞掉**（见 `TestBacklogNotDropped`）。
- 若处理时刻超过 `Scheduled + Deadline`，样本判定为**迟到**：计入看门狗
  与 `LateTotal` 统计，但不参与控制输出——既不静默丢弃，也不提前消费
  （见 `TestLateSampleNotConsumedForControl`）。

## 协议版本兼容规则

帧格式：`[magic 2B][version 1B][type 1B][payloadLen 2B][payload][crc32 4B]`，
payload 为 TLV 字段序列 `[tag 1B][len 1B][value]`。

- 版本号 `>= MinVersion` 即接受；**高版本报文可正常解码**，本端不认识的
  tag 进入 `Message.Unknown` 被忽略并保留，新增字段不影响既有处理。
- 解码失败原因可区分：`ErrTruncated`（截断）、`ErrBadMagic`、
  `ErrBadCRC`（校验失败）、`ErrUnsupportedVersion`、`ErrInvalidField`
  （字段非法）。
- 流式解码器在单帧失败后重新同步到下一个 magic，**一次失败不影响后续
  正常报文**；输入结束时调用 `Finish()` 报告残留截断帧。

## 安全降级行为

- 每路通道一个看门狗：连续错过截止时间达到 `MaxMisses`，或超过
  `Timeout` 无任何采样事件，即在当次 `Advance` 内进入 `SAFE` 状态
  （远小于约定时限的确定性触发）。
- 进入 `SAFE` 时：控制输出立即切换为 `SafeValue`，并**清除故障前的
  采样状态**（`LastValue/LastSeq` 归零），故障恢复后不沿用旧状态。
- 恢复判定只依据恢复后的新事件：收到按时样本后回到 `OK`，输出只来自
  恢复后的新采样。
- 所有导出方法持锁保护，`Snapshot()` 返回一致性快照；并发采样、控制
  与状态读取无半更新、无数据竞争（`go test -race` 验证）。

## 本地验证

```sh
go build ./...          # 构建
go run ./cmd/sensord    # 演示：正常运行 -> 停顿触发 SAFE -> 恢复 -> 总线容错
go test ./...           # 单元与集成测试
go test -race ./...     # 开启竞态检测，需稳定通过
```

测试覆盖：错过截止时间（`TestMissedDeadlinesTripSafeState`）、报文损坏
与版本兼容（`internal/protocol` 全部用例）、故障恢复
（`TestRecoveryDoesNotReuseStaleState`）、并发访问
（`TestConcurrentAccess`，需配合 `-race`）。
