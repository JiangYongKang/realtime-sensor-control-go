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
| `RecoveryFrames` | 1 | 从 SAFE 恢复所需连续**故障后新报文**数（序号严格大于恢复门槛） |

- **夹杂一帧正常报文**：任何被接受的帧（含容忍范围内乱序帧与不带序号
  的活性帧）立即清零连续异常计数（时间窗历史按 `Window` 规则保留）。
- **乱序但在容忍范围内**：帧被接受并清零异常计数，但**不推进序号基线、
  不应用载荷**（迟到旧数据不得覆盖当前输出）；之后的重复帧仍按异常计。
- **真正回退**：回退量 `baseline-seq > ReorderTolerance`，计一次异常。
- **恢复边界**：进入 SAFE 的瞬间丢弃连续异常计数、时间窗历史与恢复进度，
  通道的输出/最近值/序号也清零为安全初值；**但故障前已接受的最大序号会
  保留为“恢复门槛”**。恢复只承认故障之后真正新产生的报文。

### 从 SAFE 恢复的判定口径（哪些算恢复、哪些不算）

通道因连续不可用报文进入安全输出后，恢复必须满足：连续收到
`RecoveryFrames`（默认 1）帧**故障后新报文**。判定以故障前已接受的最大
序号 `high`（恢复门槛）为基准：

| SAFE 期间到达的报文 | 判定 | 是否推进恢复进度 | 输出/载荷 |
|---|---|---|---|
| 序号 `> high`（严格大于故障前最新序号） | 故障后新报文 | 进度 +1，门槛同步抬高到该序号 | 集齐恢复帧数解除 SAFE 后采用其值 |
| 序号 `== high`（与故障前最新序号相同，重放） | 异常（重复），计一次 `Fail` | 否，进度**清零** | 不生效 |
| 序号 `< high` 且倒退量超过 `ReorderTolerance`（比故障前更旧） | 异常（真倒退），计一次 `Fail` | 否，进度**清零** | 不生效 |
| 序号 `< high` 但在 `ReorderTolerance` 容忍内（乱序旧帧） | 接受为乱序帧、清零连续异常计数 | 否，进度**清零** | 不生效（旧数据不得覆盖输出） |
| 不带序号的活性帧 | 活性信号、清零连续异常计数 | 否（也不清零已有进度） | 不生效 |
| 坏帧（CRC/截断/版本/字段非法） | 异常 `Fail` | 否，进度**清零** | 不生效 |

要点：

- **故障前用过的任何序号都不能触发恢复，也不能把载荷写进控制输出**——
  无论它与故障前最新序号相同还是更旧；通道必须等到序号确实比故障前接受
  过的最新序号更大的报文。
- **恢复进度的计算规则确定可复现**：进度 = 当前连续“故障后新报文”的
  计数；旧报文（重复/倒退/容忍内乱序）或坏帧一旦穿插进来，进度立即归零，
  之后从下一帧真正更新的报文重新连续累计，不会被旧报文带过去；活性帧
  不影响进度。默认配置（`MaxConsecutive=3`、`RecoveryFrames=1`、
  `ReorderTolerance=0`、无时间窗）下结果完全稳定。
- 看门狗联动触发的外部 `Reset()` 仍会丢弃恢复门槛（采样链路整体重来，
  此后首帧建立全新基线）；总线自身降级走 `trip()`，**保留**门槛。
- SAFE 期间任何报文的载荷都不生效；只有在恢复帧数集齐、通道解除 SAFE
  的那一帧故障后新报文，其值才会写入输出。

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
  旧序号不能恢复（`TestRecoveryUsesFreshFramesOnly`）、
  旧报文穿插时恢复进度清零重算（`TestRecoveryProgressResetByStaleFrames`）、
  活性帧不驱动恢复（`TestRecoveryLivenessDoesNotDriveRecovery`）。
- 通道从 SAFE 恢复的判定（`internal/system`，`recovery_test.go`）：
  - 故障后用故障前旧序号（相同/更旧/更早期序号、反复重放）不能恢复也不能
    改输出（`TestRecoveryStaleFramesDoNotRecover`）；
  - 只有序号严格更大的故障后新报文才能恢复并采用其值，门槛逐次抬高
    （`TestRecoveryOnlyFreshSeqRecovers`）；
  - 旧报文与更新报文穿插时，恢复进度按“连续新帧计数、旧帧清零重算”的
    确定规则执行（`TestRecoveryProgressInterleaved`）；
  - 一路通道恢复全程不影响其他通道（`TestRecoveryChannelIsolation`）；
  - 默认配置下同一喂入序列多轮快照完全一致，确定可复现
    （`TestRecoveryDeterministicDefaultConfig`）；
  - 并发喂入（坏帧/旧帧/新帧）与状态读取下快照始终自洽，无半更新
    （`TestRecoveryConcurrentSnapshotConsistency`，配合 `-race`）；
  - 3000 次反复进入/退出 SAFE 行为稳定、计数有界、内存无积压增长
    （`TestRecoveryLongRunBounded`）。
- 通道级安全（`internal/system`）：
  - 连续坏帧降级、输出切安全值、其他通道不受影响
    （`TestBusBadFramesTripChannelSafe`）；
  - 有效报文恢复且只用恢复后新报文（`TestBusRecoveryUsesFreshFrames`）；
  - 好帧坏帧穿插（`TestBusInterleavedGoodResetsCounter`）；
  - 序号重复/倒退计异常（`TestBusDuplicateAndRegressionCountAsFaults`）；
  - 乱序容忍可配置且乱序帧不覆盖输出（`TestBusReorderToleranceConfigurable`）；
  - 复用总线按通道字段落地、无法归属帧计数（`TestBusMuxRoutingByChannel`、
    `TestBusMuxThreeBadRoutableFramesTrip`）；
  - SAFE 期间载荷抑制与恢复帧数（`TestBusSafeSuppressesPayload`）；
  - 多通道坏帧突发/巨长流隔离（`TestBusChannelIsolationUnderBurst`）；
  - 有界积压（`TestBusBoundedBacklog`）；
  - 并发总线输入、采样推进与一致性快照（`TestConcurrentBusSnapshotConsistency`、
    `TestConcurrentAccess`，配合 `-race`）；
  - 通道截断归因（`TestFinishChannelBusTruncationCharged`）。

### 本地复现“旧报文不能恢复”

```sh
# 观察恢复判定各用例的业务与结果（日志含每一步的状态/进度/输出）：
go test ./internal/system/ -run 'TestRecovery' -v
go test ./internal/busfault/ -run 'TestRecovery' -v
# 全量回归（含既有调度、协议兼容、故障恢复用例）+ 竞态检测，多轮稳定：
go test -race -count=3 ./...
```
