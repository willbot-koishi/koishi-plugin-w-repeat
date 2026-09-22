# 基于 message-db 的重构方案

## 目标

- message-db 成为原始消息、附件地址和分词结果的唯一来源。
- w-repeat 只保存“哪些消息组成了一次复读”及复读特有的状态。
- 保留现有查询指令和数字记录 ID，允许新旧记录并存。
- 迁移可恢复、可核验；无法可靠关联的旧记录不猜测、不删除。

## 当前数据与约束

本机数据库快照中：

- `w-repeat-record` 有 170,727 条记录，逻辑大小约 7.25 GB；
- 其中内嵌图片 base64 约 7.19 GB，文本总量约 5.9 MB；
- 61,127 条复读记录早于 message-db 的最早消息，无法建立真实消息引用；
- 直接按群、用户、时间对两张大表做关联查询超过两分钟仍未完成。

因此不能在每次指令调用时临时重建引用，也不能原地替换旧表。最大的重复存储来源是图片 base64。

## message-db 前置接口

### 可等待的消息捕获

仅监听 message-db 的保存完成事件不够：Satori 的 `message` 事件监听器之间没有可依赖的异步完成顺序，事件回调也不能把机器人复读内容返回给 Koishi 中间件。message-db 应提供：

```ts
captureMessage(session: Session): Promise<SavedMessage | undefined>
```

该方法完成策略检查、附件转存、规范化和落库，并返回实际保存的消息。它必须针对同一个 Session/message key 合并并发调用：message-db 自己的事件监听器和 w-repeat 中间件可能同时调用它，但只能执行一次转存和写入。现有 `message-db/message` 事件继续用于无需回复消息的观察者。

w-repeat 在自己的中间件开头 `await captureMessage(session)`，因此既能比较数据库中的规范化内容，也能继续用中间件返回值触发机器人复读。如果 message-db 因只读、未跟踪群等策略不保存消息，w-repeat 应跳过并记录一次明确警告。

### 批量读取

增加服务方法：

- `getMessagesByKeys(keys)`：按 key 批量读取，返回结果可恢复输入顺序；
- `getWordsByMessageKeys(keys)`：批量读取已有分词结果。

指令层不得逐条读取消息，避免 N+1 查询。

### 引用保留

message-db 当前默认回收旧消息。w-repeat 建立引用后必须通过通用 API 保留消息，并在删除复读记录时释放引用，例如：

```ts
retainMessages(owner, keys)
releaseMessages(owner, keys)
```

`owner` 使用 `w-repeat:<recordId>`。实现必须幂等，GC 只删除没有任何保留者的消息及分词行。不能使用容易因重试而漂移的引用计数；可在消息行保存唯一的 owner 集合，或使用独立引用表并让 GC 分批排除引用。

## v2 数据模型

### `w-repeat-record-v2`

| 字段 | 用途 |
| --- | --- |
| `id` | 延续数字记录 ID，保持指令兼容 |
| `platform`, `guildId` | 与 message-db 使用相同的群标识 |
| `starterMessageKey` | 展示内容、回复关系和分词的来源 |
| `interrupterMessageKey` | 可空；真正打断复读的消息 |
| `startTime`, `endTime` | 排序、范围筛选和日历统计索引 |
| `suspensions` | 挂起与恢复的消息 key/时间边界 |

索引至少包括 `(platform, guildId, startTime)`。

### `w-repeat-message-v2`

| 字段 | 用途 |
| --- | --- |
| `recordId`, `index` | 联合主键，保存复读消息顺序 |
| `messageKey` | 指向 `w-message-v2.key` |
| `platform`, `userId` | 小型查询索引，避免统计时关联整张消息表 |

另建 `messageKey` 和 `(platform, userId, recordId)` 索引。这里冗余用户标识是查询索引，不复制消息正文或附件。

`w-repeat-user`、`w-repeat-calendar` 和 `w-repeat-word` 只可作为可重建的物化统计，不再作为事实来源。第一版可保留用户和日历缓存；词频应读取 starter message 的 message-db 分词结果，确认性能后再决定是否保留聚合缓存。v2 不保存 `content`、`images.b64` 或内嵌 `words`。

## 在线处理流程

1. w-repeat 中间件等待 message-db 的 `captureMessage(session)`；message-db 完成 asset 转存、类型计算和落库。
2. w-repeat 对每个群串行处理返回的 SavedMessage，使用规范化元素树比较消息。
3. 状态机只在内存中保存候选 message key；一次复读结束时写入 record 和 message 两张表。
4. 写入成功后注册 message-db 保留引用；失败可按 record ID 幂等重试。
5. 机器人复读仍由当前中间件返回，避免主动发送改变中间件语义。

图片相等性应比较 asset 转存后的稳定标识，而不是临时平台 URL，也不再下载并编码 base64。若某个资产服务不能产生稳定地址，message-db 需要公开统一的内容/资产规范化函数。

## 状态机先行重构

现有状态机必须在接入 message-db 前抽成独立模块并补齐序列测试。目前至少有两处时序错误：

- 挂起记录的回调已经确认消息与挂起内容相同，却又用消息是否等于“当前记录”作为恢复条件；通常永远无法恢复，而且替换 `runtime.currentRec` 后局部变量仍指向旧记录。
- 队列候选在本次消息的 `repeatCount` 计算之后才激活，激活消息不会进入正确的用户统计；多个相同候选还会被并行处理。

目标接口应是一个纯状态转换函数，外层负责数据库副作用；同一群的事件严格串行。至少覆盖：普通复读、同一用户连续复读、队列激活、挂起恢复、超过无关消息阈值、回复一致性和机器人触发阈值。

## 历史迁移

1. 等 message-db v2 迁移完成后再开始 w-repeat 迁移。
2. 按旧记录 ID 游标分批处理并保存 checkpoint。
3. 对 message-db 覆盖时间内的记录，按平台、群、时间窗口读取候选，再用用户、规范化内容和顺序验证；时间戳相同或多候选时不得只取第一条。
4. 记录 `migrated`、`unmatched`、`ambiguous` 三类结果及原因，支持重复执行。
5. 查询层先读 v2，未迁移记录回退到 v1；核验完成前不删除 v1。
6. message-db 出现以前的 61,127 条记录保留为只读 legacy 数据。若要回收空间，单独把 base64 上传到 assets 并保存 URL；不能伪造 message-db 引用。

## 实施顺序

1. 修复并测试独立状态机，不改变存储格式。
2. 在 message-db 增加幂等捕获、批量读取和引用保留 API，并测试并发捕获与 GC。
3. 增加 v2 表和只写 v2 的新在线路径，保留 v1 查询回退。
4. 改造详情、统计、图表和词频查询，按 key 批量 hydrate。
5. 实现 checkpoint 迁移与核验报告。
6. 迁移可关联记录；最后再决定 legacy 图片资产化和 v1 清理策略。
