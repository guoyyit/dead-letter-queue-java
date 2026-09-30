# Dead Letter Queue (Java / Spring Boot)

本仓库实现了一套**不依赖任何外部消息中间件**的消息失败治理能力。消息通过 HTTP 接口入队，
消费方通过接口取出消息并回执成功/失败；失败消息按每条消息的重试策略退避重试，重试耗尽后
原子转入死信区。全部状态保存在本机嵌入式 H2 数据库中，服务重启后从最近一次已提交的进度继续。

## 核心语义

### 消息生命周期

1. 入队后消息为 `PENDING`，立即可被投递。
2. 消费方调用“取出消息”接口，消息在一个**租约（lease）**内变为 `IN_FLIGHT`。
3. 消费方在租约内回执：
   - `ack`：消息变为 `SUCCEEDED`，记录一条 `SUCCESS` 投递历史。
   - `nack`：记录一条 `FAILED` 投递历史与失败原因：
     - 尚未超过最大重试次数：消息回到 `PENDING`，直到 `visible_at = 当前时间 + 退避间隔` 才可再次投递；
     - 已达到最大重试次数：在**同一个数据库事务**内把消息置为 `DEAD` 并写入死信区，
       不存在“既不在队列也不在死信区”的窗口。
4. 若消费方崩溃、未在租约内回执，后台租约恢复任务会在租约到期后把消息重新置为 `PENDING`，
   保证至少一次投递（at-least-once）。崩溃当次没有已提交的消费结果，因此不增加尝试计数。

尝试次数（`attempts`）只按**已提交的消费结果**（ack/nack）计数，单纯的投递/重投不计数。
因此当 `maxRetries = N` 时：消息一共被消费 `N + 1` 次（首次 + N 次重试），最后一次仍失败即转死信。

### 退避策略

- `FIXED`：每次重试退避固定为 `initialBackoffMs`。
- `EXPONENTIAL`：第 `k` 次失败后退避 `initialBackoffMs * 2^(k-1)`，上限为 `maxBackoffMs`。

退避中的消息只是“不可见”，**不会阻塞同队列其他消息**：投递按 `visible_at` 最早的可见消息选取。

### 死信区

每条死信保留：

- 原始消息内容（`payload`）、所属队列；
- 最近一次失败原因（`lastReason`）；
- 首次/最近入区时间（`firstDiedAt` / `lastDiedAt`）、累计入区次数（`deathCount`）；
- **完整重试历史**（每次投递的结果、原因、起止时间）。

死信状态：

- `ACTIVE`：位于死信区，可查看/重放/清除；
- `REPLAYING`：已发起重放、消息重新进入消费流程，期间不可被再次重放或清除；
- `RESOLVED`：重放后消费成功，记录保留以便追溯，不计入死信存量，也不再参与投递。

### 重放

- 单条或批量重放会原子地把死信从 `ACTIVE` 抢占为 `REPLAYING`（条件更新，数据库行锁保证
  **同一条记录不可能被并发重放两次**：失败者得到 `409 CONFLICT`）。
- 重放会创建一条重放流水（`dead_letter_replays`，状态 `IN_PROGRESS`），把消息重置为可消费并
  按原重试策略再次投递。
- 重放后再次失败：消息与死信在同一事务内回到死信区（`ACTIVE`），`deathCount + 1`、
  重放流水置为 `FAILED`、`replayFailed` 统计 +1；同一死信行复用，历史完整可追溯。
- 重放后成功：死信置为 `RESOLVED`，重放流水置为 `SUCCEEDED`、`replayCount + 1`、
  `replaySucceeded` 统计 +1。
- **清除与重放互斥**：`REPLAYING` 的记录无法被单条/批量/整区清除（返回 `409`），
  杜绝“重放途中被清除而丢失去向”。

### 保留与自动清理

后台任务周期性执行清理：

- 超过保留时长（`dlq.retention.max-age-ms`）的死信；
- 死信总量超过上限（`dlq.retention.max-records`）时，淘汰最旧记录（按 `firstDiedAt` 升序）。

淘汰只作用于非 `REPLAYING` 的记录，因此**不会破坏正在重放或等待重试的消息**。

## HTTP 接口

所有接口前缀 `/api`。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/queues/{queue}/messages` | 入队，body：`{"payload":"...","retryPolicy":{...}}`，`retryPolicy` 可省略 |
| GET | `/api/queues/{queue}/messages/next?timeoutMs=` | 长轮询取出一条可消费消息（空时 `204`） |
| POST | `/api/queues/messages/{messageId}/ack` | 回执成功 |
| POST | `/api/queues/messages/{messageId}/nack` | 回执失败，body：`{"reason":"..."}` |
| GET | `/api/queues/messages/{messageId}` | 查看消息状态 |
| GET | `/api/queues/messages/{messageId}/attempts` | 查看完整投递/重试历史 |
| GET | `/api/dead-letters?queue=&status=&reasonContains=&fromMs=&toMs=&page=&size=` | 分页/条件查看死信 |
| GET | `/api/dead-letters/{id}` | 查看单条死信（含完整历史） |
| POST | `/api/dead-letters/{id}/replay` | 单条重放 |
| POST | `/api/dead-letters/replay` | 批量重放，body：`{"ids":[1,2]}`（重放中的自动跳过） |
| DELETE | `/api/dead-letters/{id}` | 清除单条（重放中返回 `409`） |
| POST | `/api/dead-letters/clear` | 批量清除，body：`{"ids":[1,2]}` |
| DELETE | `/api/dead-letters?queue=` | 清除全部（或指定队列）中的非重放记录 |
| GET | `/api/stats/{queue}` | 队列统计 |

`GET /api/stats/{queue}` 返回字段：

```json
{
  "totalEnqueued": 10,
  "pendingRetry": 1,
  "inFlight": 0,
  "succeeded": 7,
  "deadLetter": 2,
  "replaying": 0,
  "replaySucceeded": 3,
  "replayFailed": 1
}
```

统计全部由持久化表实时聚合，重启后与记录状态保持一致。

## 配置项（`application.properties`）

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `dlq.data-dir` | `./data/dlq` | 本地状态目录（H2 文件位置） |
| `spring.datasource.url` | `jdbc:h2:file:${dlq.data-dir}/dlqdb;...` | 嵌入式 H2 数据源 |
| `dlq.retry.max-retries` | `3` | 默认最大重试次数（首次消费失败后再试 N 次） |
| `dlq.retry.initial-backoff-ms` | `1000` | 初始退避间隔 |
| `dlq.retry.max-backoff-ms` | `60000` | 退避间隔上限 |
| `dlq.retry.backoff-type` | `EXPONENTIAL` | 退避方式：`FIXED` / `EXPONENTIAL` |
| `dlq.dispatch.lease-ttl-ms` | `30000` | 消费租约时长，超时后消息可被重新投递 |
| `dlq.dispatch.poll-timeout-ms` | `200` | 长轮询内部轮询间隔 |
| `dlq.dispatch.lease-recovery-initial-delay-ms` | `30000` | 租约恢复任务首次延迟 |
| `dlq.retention.max-age-ms` | `604800000`（7 天） | 死信保留时长 |
| `dlq.retention.max-records` | `10000` | 死信总量上限（仅计 `ACTIVE` 存量） |
| `dlq.retention.sweep-interval-ms` | `60000` | 保留清理任务执行间隔 |

入队时可用 `retryPolicy` 对单条消息覆盖以上重试相关默认值。

## 构建与测试

```bash
mvn spring-boot:run     # 启动服务
mvn test                # 运行自动化测试
```

自动化测试覆盖：重试耗尽原子转区、重启后续跑与统计一致、并发重放/清除互斥、
重放再失败回区与历史追溯、保留淘汰（年龄 + 总量、跳过重放中）、坏消息不阻塞同队列消息、
租约超时恢复以及完整 HTTP 生命周期。
