# SortMerge 源码解惑文档

生成日期: 2026-03-30

## 1. 文档说明

这份文档用于整理本轮关于 `part2 sortmerge` 的中文问答，起点是“使用点中文说明”之后的对话内容。

目标不是替代源码，而是帮助你在阅读以下代码时，更快建立“对象是怎么产生、怎么流动、什么时候从纯 sortmerge 退化为 hybrid”的整体心智模型：

- `core/src/main/java/com/jdragon/aggregation/core/job/JobContainer.java`
- `core/src/main/java/com/jdragon/aggregation/core/fusion/SortMergeFusionReader.java`
- `core/src/main/java/com/jdragon/aggregation/core/fusion/AdaptiveSortMergeFusionExecutor.java`
- `core/src/main/java/com/jdragon/aggregation/core/sortmerge/OrderedSourceCursor.java`
- `core/src/main/java/com/jdragon/aggregation/core/sortmerge/AdaptiveMergeCoordinator.java`
- `core/src/main/java/com/jdragon/aggregation/core/fusion/FusionPartitionProcessor.java`
- `core/src/main/java/com/jdragon/aggregation/core/fusion/FusionEngine.java`
- `core/src/main/java/com/jdragon/aggregation/core/consistency/service/AdaptiveSortMergeConsistencyExecutor.java`
- `core/src/main/java/com/jdragon/aggregation/core/consistency/service/ConsistencyPartitionProcessor.java`

---

## 2. 问答一：请用中文说明 `AdaptiveMergeCoordinator` 的核心变量关系

### 问题

希望用中文解释 `AdaptiveMergeCoordinator` 内部几个关键变量之间的关系，尤其是：

- `lastReadKeyBySource`
- `PendingWindow`
- `bucketUpperBound`
- `bucketMode`
- `OverflowBucketStore`

### 回答

可以把 `AdaptiveMergeCoordinator` 理解成 sortmerge 的总调度器。它不直接负责融合业务，而是负责决定每个 key 当前应该：

1. 继续留在内存里等
2. 立即交给业务处理器
3. 还是改走 overflow / hybrid

这几个变量分别承担不同职责：

#### `lastReadKeyBySource`

表示“每个 source 当前已经推进到哪个 key”。

它不是普通统计值，而是调度器判断某个 pending key 是否还能继续等待的重要依据。

例如：

- `A` 已经推进到 key=10
- `B` 已经推进到 key=20
- 当前 `PendingWindow` 里还有 key=12

那调度器就知道：`B` 不可能再补 key=12 之前的更小 key 了，因为 `B` 已经读到 20。

所以：

- `lastReadKeyBySource` 前移
- 会直接影响哪些 pending key 可以变成“可判定”

#### `PendingWindow`

表示“还不能立刻下结论的一批 key”。

每个 entry 里只保留：

- 这个 key
- 各 source 当前已知的首条记录

它不保存整条源流，只保存“未决区间”的最小必要信息。

纯 sortmerge 能否继续，核心就取决于这里是不是还能撑住。

#### `bucketUpperBound`

表示“已经被送去 overflow 处理的 key 区间上界”。

它的作用是保证一致性：

- 如果某一段 key 已经改走 overflow
- 那后续再读到不大于这个上界的 key
- 也必须继续走 overflow

否则同一段 key 会出现：

- 一部分在内存里走 sortmerge
- 一部分在磁盘回放里走 hybrid

这会让语义混乱。

#### `bucketMode`

这是总路由开关。

一旦打开，后续新来的 group 基本不再尝试进入 `PendingWindow`，而是直接写入 `OverflowBucketStore`。

所以它一变化，整条链路的推进方式就变了：

- `false`：优先走内存 sortmerge
- `true`：直接走 bucket / overflow

#### `OverflowBucketStore`

这是 hybrid / bucket 路径的中转站。

一旦某些 key 在内存里等不住了，就先把它们写到这里，后面再通过：

- `PartitionReader`
- `processPartitionPath(...)`

重新读回来并交给业务处理器。

### 一句话总结

这几个变量的联动关系可以压缩成一句：

- `lastReadKeyBySource` 决定哪些 key 可以从 `PendingWindow` 中释放
- `PendingWindow` 决定当前还能不能维持纯 sortmerge
- `bucketUpperBound` 和 `bucketMode` 决定后续 key 是否必须改走 overflow
- 一旦 overflow 开始参与，执行链路就从纯 sortmerge 进入 hybrid 或 bucket 模式

---

## 3. 问答二：从程序运行开始，到第一条记录进入，再到发生融合或发生堆积降级，流程是怎样的

### 问题

希望从程序启动开始，结合源码说明：

- 第一条记录如何进入系统
- 正常情况下如何一路走到融合
- 堆积时如何触发降级

### 回答

以 `fusion-sortmerge` 为主线，可以把运行路径先记成：

`JobContainer -> SortMergeFusionReader -> AdaptiveSortMergeFusionExecutor -> OrderedSourceCursor -> AdaptiveMergeCoordinator -> FusionPartitionProcessor -> FusionEngine -> RecordSender`

#### 第一步：作业启动

入口在 `JobContainer`。

当配置里：

- `reader.type = fusion-sortmerge`

时，`JobContainer.createBuiltinJobPlugin(...)` 会创建 `SortMergeFusionReader`。

随后：

1. 初始化 reader / writer
2. 启动 writer 线程
3. 启动 reader 线程

#### 第二步：reader 初始化执行环境

`SortMergeFusionReader.init()` 会完成这些事情：

1. 把配置解析成 `FusionConfig`
2. 创建 `DataSourcePluginManager`
3. 创建 `FusionContext`
4. 初始化 fusion 策略工厂

然后在 `startRead(...)` 里创建 `AdaptiveSortMergeFusionExecutor`。

#### 第三步：executor 创建 sortmerge 执行骨架

`AdaptiveSortMergeFusionExecutor.execute(...)` 会：

1. 为每个 source 创建一个 `OrderedSourceCursor`
2. 创建一个 `AdaptiveMergeCoordinator`
3. 让 coordinator 驱动整条 sortmerge 流程

#### 第四步：第一条记录进入系统

真正读第一条数据的不是 coordinator，而是 `OrderedSourceCursor` 内部线程。

每个 cursor 内部通过 `SourceRowScanner.scan(...)` 逐行读 source。

当第一条记录到来时：

1. `GroupAccumulator.accept(row)` 被调用
2. 计算 `OrderedKey`
3. 设置当前 group 的：
   - `currentKey`
   - `firstRow`

注意：

第一条记录此时还不会立刻参与融合。

原因是 cursor 必须先看到“下一个不同 key”或“数据源结束”，才能知道当前 key 的 group 已经完整，可以发给 coordinator。

#### 第五步：第一条记录所在 group 被正式发出

当 cursor 读到下一条不同 key 时，会触发：

- `emitCurrent()`

从而生成一个 `OrderedKeyGroup`，再包装成：

- `CursorEvent.group(...)`

并放入事件队列，交给 `AdaptiveMergeCoordinator`。

#### 第六步：coordinator 决定这条 key 走哪条路

`AdaptiveMergeCoordinator.handleEvent(...)` 收到 group 后，做几件关键事情：

1. 更新 `lastReadKeyBySource`
2. 检查当前是否已进入 `bucketMode`
3. 检查当前 key 是否落在 `bucketUpperBound` 已覆盖的区间
4. 如果没有降级，就先放入 `PendingWindow`
5. 然后尝试 `resolveReadyKeys()`

#### 第七步：正常融合何时发生

只有当 `resolveReadyKeys()` 判断“当前最小 key 已经可以确定最终参与方”时，才会把它交给业务处理器。

在 fusion 分支里，处理路径是：

1. `ResolvedGroupHandler.handle(key, firstRowsBySource)`
2. 构造只包含当前 key 的 `groups`
3. 交给 `FusionPartitionProcessor.processGroups(...)`
4. `FusionPartitionProcessor` 再调 `FusionEngine.fuseRows(...)`
5. 得到 `Record`
6. 通过 `RecordSender.sendToWriter(...)` 发给 writer

也就是说，真正“发生融合”的时刻，不是记录刚被扫描进来时，而是：

- key 已在 coordinator 中被判定可处理
- 并被送入 `FusionPartitionProcessor`

#### 第八步：什么时候会发生堆积降级

纯 sortmerge 的内存压力集中在 `PendingWindow`。

如果某些 source 很快，而某个 source 很慢，调度器就会不断积攒“还不能下结论的 key”。

当以下任一条件超过阈值时，就会触发降级：

1. `PendingWindow.size()` 超阈值
2. `PendingWindow.getEstimatedBytes()` 超阈值

#### 第九步：从 sortmerge 进入 hybrid 的方式

有两种：

##### 方式 A：`spillOldestPending()`

这是温和降级。

它只会把最老的一批 unresolved key 写入 `OverflowBucketStore`，让剩余窗口继续走内存归并。

这时执行模式记为：

- `hybrid`

##### 方式 B：`switchToBucketMode()`

这是更激进的降级。

它会：

1. 打开 `bucketMode`
2. 把当前 pending 全部刷到 overflow
3. 后续新来的 group 也直接进 bucket

#### 第十步：hybrid 后如何继续完成融合

一旦 executor 发现 `overflowBucketStore` 中已有数据，就会执行：

- `processOverflowStore(...)`

然后通过：

1. `PartitionReader`
2. `processPartitionPath(...)`

把 spill 数据重新组装成 `groups`，再交给同一个：

- `FusionPartitionProcessor`

继续处理。

也就是说，hybrid 并没有换融合算法，只是把“key 的来源”改成了 spill 回放。

---

## 4. 问答三：请继续用一个具体例子解释

### 问题

希望继续用具体例子解释：

- 第一条记录到底怎样变成可融合对象
- source 之间不齐时，怎样导致 pending 堆积
- 超限后怎样真正进入 hybrid

### 回答

下面用一个简化例子来说明。假设 join key 是 `biz_id`，有 3 个 source：

- `A`: 1, 2, 3, 4, 5, 6
- `B`: 1, 4, 5, 6
- `C`: 1, 2, 3, 4, 5, 6

并假设：

- `pendingKeyThreshold = 3`

真实项目里阈值通常远大于 3，这里只是为了把过程讲清楚。

### 场景一：key=1 正常融合

#### A 首先读到 key=1

`A` 的第一条记录 `biz_id=1` 被读出后：

1. 进入 `OrderedSourceCursor.GroupAccumulator`
2. 变成：
   - `currentKey = 1`
   - `firstRow = rowA1`

此时还不会融合。

#### A 继续读到 key=2

当 `A` 读到下一条不同 key，也就是 `2` 时：

1. `emitCurrent()` 被触发
2. 生成：
   - `OrderedKeyGroup(source=A, key=1, firstRow=rowA1)`
3. 包装成 `CursorEvent.group(...)`
4. 发送给 coordinator

coordinator 收到后：

1. 更新 `lastReadKeyBySource[A] = 1`
2. 把 key=1 放进 `PendingWindow`

这时窗口状态大概是：

- `1 -> {A}`

#### C 也读到 key=1

流程相同，窗口变成：

- `1 -> {A, C}`

仍然不能融合，因为 `B` 还没表态。

#### B 最后读到 key=1

窗口变成：

- `1 -> {A, B, C}`

这时 `resolveReadyKeys()` 判断：

- 这个 key 已经具备所有参与方

于是：

1. 从 `PendingWindow` 移除 key=1
2. 交给 `ResolvedGroupHandler`
3. 进入 `FusionPartitionProcessor`
4. 再进入 `FusionEngine.fuseRows(...)`
5. 最终产出 `Record`

到这里，key=1 才真正发生融合。

### 场景二：某个 source 缺 key，但仍可继续推进

看 key=2：

- `A` 有
- `C` 有
- `B` 没有

如果后面 coordinator 看到：

- `lastReadKeyBySource[B] = 4`

那它就知道：

- `B` 已经越过 key=2
- `B` 不会再补 key=2

于是 key=2 也能从 `PendingWindow` 中释放，继续融合。

这就是 `lastReadKeyBySource` 的推进意义：

它不是统计，而是“释放等待 key 的依据”。

### 场景三：为什么会堆积

假设现在：

- `A` 很快，连续给出 1,2,3,4
- `C` 也很快，连续给出 1,2,3,4
- `B` 非常慢，迟迟不来

这时 coordinator 只能不断把 key 放进 `PendingWindow`：

- `1 -> {A, C}`
- `2 -> {A, C}`
- `3 -> {A, C}`
- `4 -> {A, C}`

因为 `B` 没推进，这些 key 还不能判定。

一旦窗口数量超过阈值 3，就发生堆积降级。

### 场景四：超限后怎样进入 hybrid

这时 `handleEvent(...)` 看到：

- `PendingWindow.size() > pendingKeyThreshold`

如果配置策略是 `spillOldestPending()`，那么会发生：

1. 从 `PendingWindow` 里取出最老的一批 key
2. 假设这里取出 key=1,2,3
3. 把它们各自已有的 source row 写入 `OverflowBucketStore`
4. 保留 key=4 在内存窗口里继续等
5. 把执行模式标记成 `hybrid`

这时状态大概变成：

- `PendingWindow`: 只剩 `4`
- `OverflowBucketStore`: 已有 `1,2,3`
- `bucketUpperBound = 3`

### 场景五：为什么后面 B 的 key=2 不能回到内存

假设这时 `B` 终于到来，先给了 `key=2`。

coordinator 会判断：

- 当前 key `2`
- `bucketUpperBound = 3`
- 所以 `2 <= 3`

结论：

- 这个 group 不能再回到 `PendingWindow`
- 必须直接写入 `OverflowBucketStore`

这样才能保证：

- 同一段 key 的数据都在同一条回放路径里

否则会出现：

- `A/C` 的 key=2 已经 spill
- `B` 的 key=2 却还走内存

这样同一个 key 会被拆成两半处理。

### 场景六：hybrid 最后如何补完融合

当内存阶段结束后，executor 会检查：

- `overflowBucketStore` 是否有数据

如果有，就执行：

1. `processOverflowStore(...)`
2. `PartitionReader` 逐个 partition 回读数据
3. `processPartitionPath(...)` 重新把 row 按 key 组装成 `groups`
4. 再次交给 `FusionPartitionProcessor.processGroups(...)`
5. 最终继续由 `FusionEngine.fuseRows(...)` 完成融合

所以 hybrid 的本质不是“换了一套融合逻辑”，而是：

- sortmerge 负责尽量在内存里把 key 判定出来
- 判定不出来、或者窗口撑不住时，把这些 key 改走 spill
- spill 后再回放，同样交给同一套 fusion 处理器

---

## 5. 阅读源码时建议的顺序

如果你后续继续看代码，建议按下面顺序阅读，这样最容易把对象流转串起来：

1. `JobContainer`
2. `SortMergeFusionReader`
3. `AdaptiveSortMergeFusionExecutor`
4. `OrderedSourceCursor`
5. `AdaptiveMergeCoordinator`
6. `FusionPartitionProcessor`
7. `FusionEngine`

如果要看 consistency 分支，则把最后两步替换为：

6. `ConsistencyPartitionProcessor`
7. `DataComparator / ConflictResolver / UpdateExecutor`

---

## 6. 最后的浓缩结论

关于 `part2 sortmerge`，最值得记住的是下面这句话：

> 这套实现不是“先 merge sort，再另起一套 hybrid 算法”，而是“先尽量在内存窗口里完成多路排序归并判定；一旦某段 key 等不住，就把那段 key 改走 overflow，再通过 partition 回放，重新送回同一个业务处理器继续完成融合或一致性处理”。

换句话说：

- 纯 `sortmerge` 和 `hybrid` 共享同一套业务处理器
- 它们真正不同的地方，不在“怎么融合”
- 而在“当前 key 是在内存窗口里被判定出来的，还是在 spill 后被回放出来的”

