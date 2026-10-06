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
| `internal/protocol` | 带版本的总线报文编解码（TLV + CRC32），有界流式缓冲 |
| `internal/safety` | 看门狗与安全降级状态机 |
| `internal/busfault` | 通道级总线报文异常计数、乱序容忍与恢复状态机 |
| `internal/system` | 编排层：采样、控制输出、总线接入、通道隔离、状态快照 |
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

## 总线接入与通道级安全语义

总线不再只是“调用方能拿到错误”：报文直接进入每路通道的安全判定。

### 两种总线入口

| 入口 | 归因方式 |
|---|---|
| `System.FeedBus(data)` | **复用总线**。解码成功且带合法 `TagChannel` 字段的帧按通道落地；解码失败、无通道字段、通道名未配置的帧计入 `Snapshot().BusUnrouted`，不拖垮任何通道 |
| `System.FeedChannelBus(id, data)` / `FinishChannelBus(id)` | **通道专用总线**。该流上的每一帧、每一个解码错误（坏 CRC、截断、版本非法、字段非法）都确定归属到该通道 |

- 带通道信息的报文按 `TagChannel` 更新对应通道：`TypeControl`+`TagValue`
  更新控制输出，`TypeSample`+`TagValue` 更新该通道最近总线采样值
  （`BusLastValue`），均带序号校验。
- 通道是否安全由看门狗与总线监控**共同决定**：任一处于 `SAFE`，输出即
  锁定为 `SafeValue`，且总线报文载荷不生效；只有两者都恢复后才解除。
- 通道专用总线上连续坏帧会在**阈值帧数内**（默认 3 帧，与采样超时无关，
  不必等看门狗兜底）让该通道切到安全输出；其他通道与后续正常报文不受
  影响。

### 什么算一次异常（`busfault.Monitor`）

以下情况各计一次异常（`Fail`）：

1. 结构不完整（截断，含 `FinishChannelBus` 残留与解码器缓冲超限丢弃）；
2. 完整性校验失败（CRC）；
3. 版本不接受（`< MinVersion`）；
4. 字段不合法（已知 tag 长度非法等）；
5. 序号**重复**（与最近接受序号相同）或**倒退超过容忍量**。

### 连续异常、夹杂正常帧与恢复（确定、可配置）

`ChannelConfig.Bus`（`busfault.Config`，零值经 `Normalize` 取默认值）：

| 配置项 | 默认 | 含义 |
|---|---|---|
| `MaxConsecutive` | 3 | 连续无法接受报文的降级帧数阈值 |
| `Window` | 0（关闭） | >0 时改为滑动时间窗判定：窗口内异常累计达到 `MaxConsecutive` 即降级，**不要求严格连续**；帧间夹杂正常帧不清窗，异常随时间滑出窗口（在 `Advance` 时重评） |
| `ReorderTolerance` | 0 | 允许的序号回退量（0 表示任何倒退都算异常） |
| `RecoveryFrames` | 1 | 从 SAFE 恢复所需连续有效报文数 |

- **夹杂一帧正常报文**：任何被接受的帧（含容忍范围内乱序帧与不带序号
  的活性帧）立即清零连续异常计数（时间窗历史按 `Window` 规则保留）。
- **乱序但在容忍范围内**：帧被接受并清零异常计数，但**不推进序号基线、
  不应用载荷**（迟到旧数据不得覆盖当前输出）；之后的重复帧仍按异常计。
- **真正回退**：回退量 `baseline-seq > ReorderTolerance`，计一次异常。
- **恢复边界**：进入 SAFE 的瞬间，故障前已接受的最大序号被记为
  **故障水位**，同时丢弃序号基线、连续计数与恢复进度（通道的输出/
  最近值/序号也清零为安全初值）。
- **什么算有效恢复帧**：恢复期间只有序号**严格大于故障水位**的报文
  （即确实是故障之后新产生的报文）才算有效恢复帧，连续集满
  `RecoveryFrames` 帧后通道回到 OK 并采用其载荷。
- **什么不算**：重复序号、倒退序号、以及故障前已出现过的任何序号
  （`seq <= 故障水位`）一律按一次异常处理——既不推进恢复进度，也不
  改动输出，并把已积累的恢复进度**清零重新计算**；因此先来旧报文、
  后来真正新报文时，恢复进度从旧报文之后重新集齐，不会被旧报文
  “带过去”。故障水位随每次进入 SAFE 时已接受的最大序号单调抬升，
  多次进出安全状态后判定依然确定、可复现。
- SAFE 期间恢复进度被新异常打断时重新计数；期间报文载荷一律不生效。

### 多通道并发与容量假设

- 每路通道持有独立解码器与 `busfault.Monitor`；**解码在全局状态锁之外**
  进行（通道各自一把解码锁，复用总线一把解码锁），一路通道的坏帧突发、
  长流式输入只阻塞自己，不停住其他健康通道的采样与控制。
- 状态应用阶段短暂持有全局写锁；`Snapshot()` 持读锁拷贝全部字段，
  **任何时刻读到的都是一致快照，不会出现半更新**。
- 每个解码器缓冲有界（`protocol.DefaultMaxBuffered = 256KiB`，可用
  `protocol.NewBoundedDecoder` 自定义）：残留与新输入合计超限时丢弃
  残留并报告一次 `ErrTruncated`，随后正常帧继续解码。因此单次
  `FeedBus/FeedChannelBus` 建议输入不超过约 256KiB（约 4 个最大帧），
  长流请分块喂入；异常恢复后内存占用不增长（见
  `TestBusBoundedBacklog`、`TestBusChannelIsolationUnderBurst`）。
- 时间窗口异常时刻、各类计数器均为定长/按窗裁剪的结构，长时间运行
  无无限增长。

## 本地验证

```sh
go build ./...          # 构建
go run ./cmd/sensord    # 演示：正常运行 -> 停顿触发 SAFE -> 恢复 ->
                        #       总线容错 -> 通道级坏帧降级与恢复
go test ./...           # 单元与集成测试
go test -race ./...     # 开启竞态检测，需稳定通过
go test -race -count=N ./...  # 多轮重复，验证并发用例稳定可复现
```

测试覆盖（除既有调度/协议/看门狗用例外）：

- 看门狗：错过截止时间（`TestMissedDeadlinesTripSafeState`）、
  恢复不沿用旧状态（`TestRecoveryDoesNotReuseStaleState`）、
  迟到样本不参与控制（`TestLateSampleNotConsumedForControl`）。
- 协议：报文损坏重同步、版本兼容、未知字段保留、字段非法、
  缓冲超限丢弃后恢复（`TestBoundedDecoderDropsOverrunAndRecovers`）。
- 总线异常计数与恢复（`internal/busfault`）：连续坏帧触发、正常帧清零、
  多帧恢复与异常打断、乱序容忍与真倒退、时间窗模式与过期遗忘、
  恢复只认序号大于故障水位的新报文（`TestRecoveryUsesFreshFramesOnly`）、
  旧报文夹杂时恢复进度清零重计（`TestRecoveryProgressRestartedByStaleFrames`）、
  故障水位跨多次故障单调抬升（`TestFaultWatermarkAdvancesAcrossEpisodes`）。
- 通道级安全（`internal/system`）：
  - 连续坏帧降级、输出切安全值、其他通道不受影响
    （`TestBusBadFramesTripChannelSafe`）；
  - 有效报文恢复且只用恢复后新报文（`TestBusRecoveryUsesFreshFrames`）；
  - 故障后旧序号报文不能恢复也不能改输出，只有真正更新的报文才能恢复
    （`TestBusStaleSeqCannotRecoverNorDriveOutput`）；
  - 旧报文与新报文穿插时恢复进度清零重计
    （`TestBusRecoveryProgressWithInterleavedStale`）；
  - 一路通道恢复判定不影响其他通道（`TestBusRecoveryIsolationAcrossChannels`）；
  - 好帧坏帧穿插（`TestBusInterleavedGoodResetsCounter`）；
  - 序号重复/倒退计异常（`TestBusDuplicateAndRegressionCountAsFaults`）；
  - 乱序容忍可配置且乱序帧不覆盖输出（`TestBusReorderToleranceConfigurable`）；
  - 复用总线按通道字段落地、无法归属帧计数（`TestBusMuxRoutingByChannel`、
    `TestBusMuxThreeBadRoutableFramesTrip`）；
  - SAFE 期间载荷抑制与恢复帧数（`TestBusSafeSuppressesPayload`）；
  - 多通道坏帧突发/巨长流隔离（`TestBusChannelIsolationUnderBurst`）；
  - 有界积压（`TestBusBoundedBacklog`）；
  - 并发总线输入、采样推进与一致性快照（`TestConcurrentBusSnapshotConsistency`、
    `TestConcurrentStaleRecoverySnapshotConsistency`、`TestConcurrentAccess`，
    配合 `-race`）；
  - 通道截断归因（`TestFinishChannelBusTruncationCharged`）。
