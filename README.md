# dead-letter-queue-java

基于 Spring Boot 的本地死信队列服务：消息发布、拉取式投递、失败重试（退避等待）、死信区管理与重放，全部状态持久化到本地 JSON 文件，重启后自动恢复。

## 快速开始

```bash
mvn spring-boot:run
```

服务启动后监听 8080 端口，状态文件写入 `dlq.store-dir`（默认 `data/dlq/dlq-state.json`）。

## 核心语义

### 重试触发时机

- 消息通过 `POST /api/queues/{queue}/poll` 被取出后进入消费中（IN_FLIGHT）状态，并持有可见性租约。
- 消费方调用 `POST /api/queues/messages/{messageId}/nack` 回执失败时触发重试判定，**对任何消息生效**，无论它是新发布的还是死信区重放出来的：
  - 已重试次数未耗尽且失败原因可重试：消息进入退避等待（RETRYING），到期后自动重新投递。
  - 重试次数耗尽，或失败原因被判定为不可重试：消息原子转入死信区，死信记录保留原始内容、完整重试历史（每次投递的失败时间、原因、退避时长）与最终失败原因。
- 消费成功调用 `POST /api/queues/messages/{messageId}/ack`，消息被移除；若消息来自死信重放，对应死信记录同时置为已解决（RESOLVED）。
- 同一条消息的回执只生效一次：已回执或已被租约恢复推进的消息再次回执时，接口返回 `409 CONFLICT` 明确拒绝，不改变消息状态、不重复累计重试次数、不产生重复死信记录。失败回执与租约恢复并发时，同一消息至多被一方推进。

### 退避计算规则

第 `n` 次重试（首次失败后 `n = 1`）前的等待时长按策略类型计算，并不超过 `maxIntervalMillis` 上限：

| 退避方式 | 第 n 次等待时长 |
| --- | --- |
| `FIXED` | `initialIntervalMillis` |
| `LINEAR` | `initialIntervalMillis * n` |
| `EXPONENTIAL` | `initialIntervalMillis * 2^(n-1)` |

策略来源：发布消息时可携带 `maxRetries / initialIntervalMillis / maxIntervalMillis / backoffType`，缺省字段回落到全局默认配置（见下文配置项）。`maxRetries = 0` 表示首次失败直接转入死信区。

### 不可重试判定约定

失败原因（nack 请求体的 `reason`）以 `NON_RETRYABLE:` 前缀开头时，该次失败被判定为不可重试，跳过重试直接转入死信区。

### 重启续跑语义

- 所有状态变更在单锁内完成并立即落盘（临时文件 + 原子移动），重启后从状态文件恢复。
- 退避等待中的消息记录的是绝对到期时间，重启后**保留剩余等待时长**，不重新计时、不提前投递。
- 重启前已被取出但未回执的消息，在租约到期后由后台维护任务恢复为可投递状态，投递与重试计数**连续累计**，不清零、不跳跃。
- 统计（待重试 / 消费中 / 死信 / 重放成功 / 重放失败）全部由当前持久化记录实时派生，重启后天然一致。

## 配置项

| 配置项 | 默认值 | 含义 |
| --- | --- | --- |
| `dlq.store-dir` | `data` | 本地状态存储目录 |
| `dlq.visibility-timeout-millis` | `30000` | 消息可见性超时（租约），超时未回执将重新投递 |
| `dlq.maintenance-interval-millis` | `1000` | 后台维护扫描间隔（租约回收、重放终态推进、保留淘汰） |
| `dlq.retention-millis` | `0` | 死信保留时长，`0` 表示不按时间淘汰 |
| `dlq.max-dead-records` | `0` | 死信区总量上限，`0` 表示不限制 |
| `dlq.default-max-retries` | `3` | 默认最大重试次数（首次失败后的重试次数） |
| `dlq.default-initial-interval-millis` | `1000` | 默认首次退避间隔 |
| `dlq.default-max-interval-millis` | `60000` | 默认退避间隔上限 |
| `dlq.default-backoff-type` | `EXPONENTIAL` | 默认退避方式：`FIXED` / `LINEAR` / `EXPONENTIAL` |

## 接口概览

| 方法与路径 | 说明 |
| --- | --- |
| `POST /api/queues/{queue}/messages` | 发布消息，可携带重试策略 |
| `POST /api/queues/{queue}/poll` | 非阻塞取出一条可消费消息 |
| `POST /api/queues/messages/{messageId}/ack` | 消费成功回执 |
| `POST /api/queues/messages/{messageId}/nack` | 消费失败回执（`{"reason": "..."}`） |
| `GET /api/queues/stats` | 统计快照 |
| `GET /api/dead-letters` | 死信记录列表（支持队列、状态、条数过滤） |
| `GET /api/dead-letters/{id}` | 死信记录详情 |
| `POST /api/dead-letters/replay` | 批量重放死信记录 |
| `POST /api/dead-letters/{id}/replay` | 重放单条死信记录 |
| `DELETE /api/dead-letters` | 批量清除死信记录 |
| `DELETE /api/dead-letters/{id}` | 清除单条死信记录 |

## 最小验证流程（发布 → 重试 → 死信区）

```bash
# 1. 发布一条最多重试 2 次、固定退避 1 秒的消息
curl -X POST localhost:8080/api/queues/orders/messages \
  -H 'Content-Type: application/json' \
  -d '{"payload":"hello","maxRetries":2,"initialIntervalMillis":1000,"backoffType":"FIXED"}'
# → 记录返回的消息 id

# 2. 取出消息
curl -X POST localhost:8080/api/queues/orders/poll

# 3. 回执失败：返回 RETRY_SCHEDULED，消息进入退避等待
curl -X POST localhost:8080/api/queues/messages/<id>/nack \
  -H 'Content-Type: application/json' -d '{"reason":"boom"}'

# 4. 查看统计：pendingRetry 为 1，dead 为 0
curl localhost:8080/api/queues/stats

# 5. 重复步骤 2-3：退避到期后再次取出并回执失败
#    第 3 次失败（重试耗尽）时 nack 返回 DEAD_LETTER 及死信记录

# 6. 查看死信区：记录包含原始内容、完整重试历史与最终失败原因
curl localhost:8080/api/dead-letters
```

## 测试

```bash
mvn test
```

测试覆盖：首次失败按策略重试、重试耗尽转区、零次重试与不可重试原因直转、三种退避计算与上限、退避等待中重启续跑、租约到期恢复、重复回执拒绝、失败回执与租约恢复竞争、长退避不阻塞同队列消息、批量消息按到期时间依次投递、统计与记录一致性。
