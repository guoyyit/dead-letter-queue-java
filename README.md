# 本地死信队列（Java / Spring Boot）

带可见性租约、失败退避重试与死信区的轻量单机消息队列。状态持久化在本地 JSON 文件中，
重启后自动续跑。

## 核心流程

1. `POST /api/queues/{queue}/messages` 发布消息，可携带消息级重试策略，缺省使用全局默认策略。
2. `POST /api/queues/{queue}/poll` 非阻塞取出一条可消费消息。取出后消息进入 `IN_FLIGHT`，
   携带租约到期时间 `leaseExpiresAt`。
3. 消费成功调用 `POST /api/queues/messages/{id}/ack`，消息被移除；若是死信重放消息，
   对应死信记录置为 `RESOLVED`。
4. 消费失败调用 `POST /api/queues/messages/{id}/nack`，按重试策略决定退避等待或转入死信区。

## 重试触发时机

**任何消息**（无论新发布还是从死信区重放）在一次失败回执后，都按消息携带的或全局默认的
重试策略处理：

- 未耗尽重试次数且失败原因可重试：记录一次失败历史，进入 `RETRYING` 退避等待，
  到期后自动可被再次投递。
- 重试次数耗尽（已失败投递次数 = `maxRetries + 1`）：原子转入死信区。
- 失败原因被判定为不可重试：跳过重试，首次失败即转入死信区。
- `maxRetries = 0`：首次失败直接转入死信区。

死信记录保留：原始消息内容（`originalPayload`）、来源队列、原消息 id、
完整失败重试历史（每次投递的序号、时间、原因、退避时长，转区那一次退避为 `-1`）、
最终失败原因（`lastFailureReason`）与首次/最近转区时间。

死信区记录可通过重放接口放回原队列；重放消息与普通消息遵守**完全相同**的重试语义。

## 退避计算规则

设首次失败后第 1 次重试前等待 `initialIntervalMillis`，三种方式对第 n 次失败（n 从 1 开始）：

- `FIXED` 固定：`initial`
- `LINEAR` 线性：`initial * n`
- `EXPONENTIAL` 指数：`initial * 2^(n-1)`

三种方式的结果都受 `maxIntervalMillis` 间隔上限约束（取计算值与上限的较小者）。
退避到期时间以**绝对时间戳** `nextReadyAt = 失败时刻 + 退避时长` 持久化。

## 不可重试判定约定

失败原因以 `NON_RETRYABLE:` 开头即判定为不可重试（见 `RetryPlanner.NON_RETRYABLE_PREFIX`），
例如请求体 `{"reason":"NON_RETRYABLE:poison-message"}`。此类消息不计退避，直接转入死信区。
其他任何原因（含空原因，归一化为 `unknown`）都按正常失败处理。

## 重复回执与租约恢复竞争

- 所有状态变更在同一把写锁内完成并立即落盘。
- 同一条消息的失败回执只有第一次生效：重复 `nack` 返回 `409 Conflict` 并被明确拒绝，
  不改变状态、不重复累计失败次数、不产生重复死信记录。
- 租约到期后由后台维护（或显式恢复）把消息退回可投递状态，恢复不计为一次失败、不增加投递序号。
- 失败回执与租约恢复并发时，先到的一方推进，另一方得到明确的冲突拒绝（`409`），
  同一条消息至多被推进一次。

## 重启续跑语义

- 退避等待中的消息：重启后沿用持久化的绝对到期时间 `nextReadyAt`，**不重新计时**，
  剩余等待时长保留，不会被提前投递。
- 重启前已取出未回执的消息：若重启后租约仍有效则继续占用；租约到期后恢复投递，
  投递/失败序号连续累计，不清零、不跳跃。
- 统计数据全部由持久化记录实时派生，重启后与记录天然一致。

## 长退避不阻塞其他消息

`poll` 每次选取队列中已到期（`nextReadyAt <= now`）、且不在投递中的最早就绪消息。
某条消息长时间退避等待不影响同队列其他消息的取出与回执。

## 配置项（前缀 `dlq`，见 `application.properties`）

| 配置项 | 含义 | 默认值 |
| --- | --- | --- |
| `dlq.store-dir` | 状态 JSON 文件目录 | `data/dlq` |
| `dlq.visibility-timeout-millis` | 投递租约时长，超时未回执可恢复投递 | `30000` |
| `dlq.maintenance-interval-millis` | 后台维护扫描间隔 | `1000` |
| `dlq.retention-millis` | 死信保留时长，`0` 不按时间淘汰 | `0` |
| `dlq.max-dead-records` | 死信区总量上限，`0` 不限制 | `0` |
| `dlq.default-max-retries` | 全局默认最大重试次数 | `3` |
| `dlq.default-initial-interval-millis` | 全局默认首次退避间隔 | `1000` |
| `dlq.default-max-interval-millis` | 全局默认退避间隔上限 | `60000` |
| `dlq.default-backoff-type` | 全局默认退避方式 `FIXED`/`LINEAR`/`EXPONENTIAL` | `EXPONENTIAL` |

发布消息时可在请求体中携带消息级策略字段 `maxRetries`、`initialIntervalMillis`、
`maxIntervalMillis`、`backoffType`；未提供的字段回退到全局默认值。

## 接口一览

- `POST /api/queues/{queue}/messages` 发布消息
- `POST /api/queues/{queue}/poll` 取出消息（无消息返回 `204`）
- `POST /api/queues/messages/{id}/ack` 成功回执
- `POST /api/queues/messages/{id}/nack` 失败回执，响应 `outcome` 为 `RETRY_SCHEDULED` 或 `DEAD_LETTER`
- `GET /api/queues/stats` 统计快照（待重试、投递中、死信、重放成功/失败计数及分队列统计）
- `GET /api/dead-letters` / `GET /api/dead-letters/{id}` 查看死信记录
- `POST /api/dead-letters/replay` / `POST /api/dead-letters/{id}/replay` 重放回原队列
- `DELETE /api/dead-letters` / `DELETE /api/dead-letters/{id}` 清除死信态记录

## 最小验证流程（发布 → 退避重试 → 死信区）

以最大重试次数 1、固定退避 1000ms 为例：

```bash
# 1. 发布
curl -s -X POST localhost:8080/api/queues/demo/messages \
  -H 'Content-Type: application/json' \
  -d '{"payload":"hello","maxRetries":1,"initialIntervalMillis":1000,"backoffType":"FIXED"}'
# -> {"id":"<id>", ...}

# 2. 取出（第一次投递，delivered=1）
curl -s -X POST localhost:8080/api/queues/demo/poll

# 3. 第一次回执失败 -> 进入退避等待
curl -s -X POST localhost:8080/api/queues/messages/<id>/nack \
  -H 'Content-Type: application/json' -d '{"reason":"boom"}'
# -> {"outcome":"RETRY_SCHEDULED","deadLetter":null}

# 4. 立即 poll 返回 204；统计中 pendingRetry=1、dead=0
curl -s localhost:8080/api/queues/stats

# 5. 约 1 秒后再次取出（delivered=2），再次失败 -> 转入死信区
curl -s -X POST localhost:8080/api/queues/demo/poll
curl -s -X POST localhost:8080/api/queues/messages/<id>/nack \
  -H 'Content-Type: application/json' -d '{"reason":"boom again"}'
# -> {"outcome":"DEAD_LETTER","deadLetter":{ ... 完整重试历史 ... }}

# 6. 查看死信记录与统计（dead=1）
curl -s localhost:8080/api/dead-letters?queue=demo
```

## 测试

```bash
mvn -o test
```

验收测试见
`src/test/java/com/github/guoyyit/deadletterqueuejava/tests/RetryBehaviorTest.java`，
覆盖首次失败按策略退避、重试耗尽转区、零次重试与不可重试直转、三种退避计算、
退避中重启续跑、租约恢复续跑、重复回执拒绝、失败回执与租约恢复竞争、
长退避不阻塞同队列、批量按到期依次投递、死信重放回归与重启后统计一致。
