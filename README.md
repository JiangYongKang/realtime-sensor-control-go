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
| `internal/protocol` | 带版本的总线报文编解码（TLV + CRC32）与按序帧事件 |
| `internal/safety` | 看门狗与安全降级状态机 |
| `internal/busguard` | 每路通道的总线报文异常计数、乱序容忍与恢复门 |
| `internal/system` | 编排层：采样、控制输出、总线路由、状态快照 |
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
- 解码器提供 `FeedEvents`：按字节流中的**真实到达顺序**输出 `Event{Msg, Err}`
  （好/坏帧互斥），系统据此把异常帧与正常帧按序计入对应通道，穿插场景
  的连续异常计数确定、不乱序。错误包装为 `*protocol.FrameError`，在 CRC
  失败但 payload 的 TLV 仍完整可读时**尽力携带该帧声明的通道名**，使坏帧
  也能归属到具体通道；TLV 结构损坏、通道字段缺失/重复/长度非法则无法归属。
- 单解码器等待完整帧的缓冲恒不超过 `MaxBufferedFrame`（帧头+最大 payload
  +CRC，约 64KiB+10B）；大段垃圾重新同步后立即清缓冲，长流式输入与积压
  有确定的内存边界。

## 总线接入与通道级安全语义

报文经 `System.FeedBus` 按**通道名 TLV（`TagChannel`）路由**到具体通道；
属于某通道的好帧更新该通道的采样展示与控制输出，属于它的坏帧参与它的
总线安全判定。通道最终状态由**采样看门狗与总线安全判定取或**：任一失效
即为 `SAFE`。

- **一次异常**：结构不完整（截断/坏 magic）、CRC 校验失败、版本不接受、
  字段非法（缺序号/值、值为 NaN、未知报文类型），以及序号重复或明显倒退。
- **触发降级（确定、可配置，默认值稳定可复现）**：连续不可接受报文达到
  `MaxConsecutiveBad` 帧（默认 3），**或**从首个异常起持续 `FaultWindow`
  （默认 1s）仍无可用报文，即在当次 `FeedBus` / `Advance` 内让该通道切到
  `SafeValue`，**不等待采样超时兜底**。
- **清零规则**：只有序号**严格前进**的可接受报文才清零连续异常计数；
  容忍窗口内的乱序帧既不计数也不清零。通道尚未建立序号基线前的坏帧，
  在首个可用报文建基线时一并清零。
- **乱序容忍**：序号回退幅度在 `1..ReorderWindow`（默认 1）以内判为乱序，
  帧被忽略但不计异常；回退幅度更大判为“明显倒退”，计一次异常；序号与上一
  接受序号相同判为重复，计一次异常。
- **恢复门**：进入 SAFE 时记录故障前接受序号峰值 `floor`；SAFE 期间序号
  `<= floor` 的报文一律视为陈旧（`Stale`）丢弃，只有序号严格更新的新报文
  才恢复 `OK` 并被采用。因此恢复后**只能使用恢复之后的新报文**，故障前的
  输出、序号与累计连续状态不会被沿用（进入 SAFE 时输出切安全值、
  `LastValue/LastSeq` 归零；恢复后首个新报文重建基线）。
- **不可归属帧**：无通道字段、通道名未知、或无法可靠判定通道的错误，计入
  快照 `UnroutableTotal`，**不影响任何已配置通道**。

配置项位于 `ChannelConfig.Bus`（`busguard.Config`）：`MaxConsecutiveBad`、
`FaultWindow`、`ReorderWindow`，零值字段统一填充上述默认值。

## 多通道并发与容量假设

- 每路通道独立持有看门狗与 `busguard.Guard`，计数/序号/恢复门互不共享；
  一路的坏帧突发只动它自己的计数器，其他健康通道的采样与输出不停滞。
- `FeedBus` 把**整批字节放在同一把写锁内按序处理完**，`Advance` 与
  `Snapshot()` 使用同一把锁；快照在读锁下整表拷贝，读到的永远是一致快照，
  不存在半更新（`go test -race` 稳定通过）。
- 所有通道共用一个有界流解码器，单帧缓冲不超过 `MaxBufferedFrame`，因此
  一路通道的长流式输入或坏帧积压不会让内存无界增长，也不会饿死其他通道。

## 安全降级行为

- 每路通道两路独立判定：采样看门狗（错过截止时间/静默超时）与总线安全
  判定（连续坏帧/异常时间窗/明显倒退），任一失效即在当次处理内进入 `SAFE`。
- 进入 `SAFE` 时：控制输出立即切换为 `SafeValue`，并**清除故障前的
  采样状态**（`LastValue/LastSeq` 归零），总线侧装恢复门（记录序号峰值）；
  故障恢复后不沿用旧值、旧序号。
- 恢复判定只依据恢复后的新事件：采样侧收到按时样本、总线侧收到序号严格
  新于故障前峰值的报文，才回到 `OK`，输出只来自恢复之后的新数据。
- 所有导出方法持锁保护，`Snapshot()` 返回一致性快照；并发采样、总线输入
  与状态读取无半更新、无数据竞争（`go test -race` 验证）。

## 本地验证

```sh
go build ./...          # 构建
go run ./cmd/sensord    # 演示：正常运行 -> 停顿 SAFE -> 恢复 -> 总线路由降级与恢复
go test ./...           # 单元与集成测试
go test -race ./...     # 开启竞态检测，需稳定通过
```

测试覆盖：错过截止时间（`TestMissedDeadlinesTripSafeState`）、报文损坏
与版本兼容（`internal/protocol` 全部用例）、故障恢复
（`TestRecoveryDoesNotReuseStaleState`）、并发访问
（`TestConcurrentAccess`，需配合 `-race`），以及本轮新增的总线接入场景
（`internal/system/bus_test.go`）：

- 连续坏帧当批内降级（`TestConsecutiveBadBusFramesTripChannel`）；
- 只接受恢复后新序号报文（`TestChannelRecoversOnlyWithFreshFrames`）；
- 坏帧/正常帧穿插清零（`TestBadAndGoodFramesInterleaved`）；
- 乱序容忍、重复与明显倒退（`TestReorderWithinTolerance`、
  `TestDuplicateSeqCountedAsFault`）；
- 多通道隔离与不可归属帧不误伤（`TestMultiChannelIsolation`、
  `TestUnroutableFramesDoNotTripAnyChannel`）；
- 总线异常时间窗不依赖采样超时（`TestFaultWindowTripsWithoutSampleTimeout`）；
- 并发总线输入、采样推进与一致快照（`TestConcurrentBusAndSnapshotsConsistent`）；
- 大段垃圾后缓冲有界、后续正常帧立即可处理（`TestBusBacklogBounded`）。
`internal/busguard` 另有规则级单测（帧数/时间窗双阈值、清零、乱序、
恢复门、默认值）。
