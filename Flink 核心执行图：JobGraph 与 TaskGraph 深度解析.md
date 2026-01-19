# Flink 核心执行图：JobGraph 与 TaskGraph 深度解析

在 Flink 中，**JobGraph** 和 **TaskGraph** 是作业从「用户代码」到「分布式执行」的两个关键执行图，分别对应**逻辑执行计划**和**物理执行计划**。理解两者的关系、生成过程和核心差异，是掌握 Flink 作业调度与性能优化的关键。

## 一、核心概念与整体流程

### 1. 执行图层级关系

Flink 作业的执行图分为 **4 层**，从用户代码到最终执行的 Task 逐级优化：

```Plain Text

用户代码（DataStream API） → StreamGraph → JobGraph → TaskGraph → 运行中的 Task
```

|**层级**|**生成方**|**核心作用**|
|---|---|---|
|StreamGraph|Client|基于用户代码生成的**最初逻辑图**，保留所有算子和数据流关系，未做优化|
|**JobGraph**|Client|对 StreamGraph 优化后的**提交级逻辑图**，合并算子链，是 Client 提交给 JobManager 的核心数据结构|
|**TaskGraph**|JobMaster|对 JobGraph 进一步优化的**物理执行图**，根据并行度拆分算子为 Task，是 TaskManager 执行的最终依据|
|Task|TaskManager|TaskGraph 的并行实例，是 Flink 作业的最小执行单元|
### 2. 核心目标

- **JobGraph**：**优化逻辑执行计划**，减少算子间数据传输，降低网络开销（核心是**算子链合并**）。

- **TaskGraph**：**生成物理执行计划**，根据并行度将算子拆分为可执行的 Task，分配到 TaskManager 的 Slot 中运行。

## 二、JobGraph：提交给集群的逻辑执行图

### 1. 生成过程（Client 端完成）

JobGraph 由 Client 对 `StreamGraph` 进行**优化生成**，核心优化手段是 **算子链合并（Operator Chaining）**。

#### （1）算子链合并的规则

Flink 会自动将满足以下条件的相邻算子合并为一个**算子链**，最终对应 JobGraph 中的一个 `JobVertex`：

1. 算子间是**一对一（One-to-One）** 的数据流关系（如 `map` → `filter`，无数据重分区）；

2. 算子的**并行度相同**；

3. 算子的**链策略允许合并**（默认 `ALWAYS`，可通过 API 调整）；

4. 算子在**同一个 Slot 共享组**（默认同一组）。

#### （2）量化场景示例

用户编写的行情处理代码：

```Java

DataStream<MarketData> stream = env.addSource(new KafkaSource<>()) // 算子1：Kafka Source
    .map(new MarketDataMapFunction()) // 算子2：解析行情
    .filter(new ValidFilterFunction()) // 算子3：过滤无效行情
    .keyBy(MarketData::getSymbol) // 触发重分区
    .window(TumblingEventTimeWindows.of(Time.minutes(1))) // 算子4：窗口计算
    .aggregate(new PriceAggregateFunction()); // 算子5：聚合涨幅
```

**StreamGraph → JobGraph 的优化过程**：

- 算子1（Source）、算子2（map）、算子3（filter）满足合并条件 → 合并为一个 `JobVertex`（命名为 `Source-Map-Filter`）；

- `keyBy` 触发**数据重分区（Shuffle）**，打破算子链；

- 算子4（window）、算子5（aggregate）满足合并条件 → 合并为另一个 `JobVertex`（命名为 `Window-Aggregate`）；

- 最终 JobGraph 包含 **2 个 JobVertex**，比 StreamGraph 的 5 个算子大幅减少。

#### （3）手动控制算子链（量化场景调优）

在量化策略中，若某个算子（如复杂指标计算）耗时较长，可手动禁用算子链，让其独立执行，避免阻塞其他算子：

```Java

// 禁用 filter 之后的算子链
stream.filter(new ValidFilterFunction())
    .disableChaining() // 后续算子不再与当前算子合并
    .keyBy(...);

// 强制某个算子作为链的起点（不与前面的算子合并）
stream.map(new MarketDataMapFunction())
    .startNewChain() // 从当前算子开始新的算子链
    .filter(...);
```

### 2. JobGraph 的核心结构

JobGraph 的核心组成是 `JobVertex`（对应算子链）和 `JobEdge`（对应算子链间的数据流），包含以下关键信息：

1. **JobVertex**：每个 JobVertex 对应一个合并后的算子链，存储算子的逻辑信息（如 Source 配置、聚合函数）；

2. **JobEdge**：描述 JobVertex 之间的数据流关系，标记是否需要重分区（如`keyBy` 对应的 Edge 会标记为 `SHUFFLE`）；

3. **优化信息**：算子链合并的规则、并行度配置、资源需求（如内存大小）；

4. **辅助信息**：作业 ID、作业名称、状态后端配置等。

### 3. 核心作用

- 作为 Client 与 JobManager 之间的**通信载体**，Client 将 JobGraph 提交给 JobManager 的 Dispatcher；

- 保留了逻辑执行计划的核心优化，减少了后续调度的复杂度。

## 三、TaskGraph：TaskManager 执行的物理执行图

### 1. 生成过程（JobMaster 端完成）

JobMaster 接收 Client 提交的 JobGraph 后，会进一步优化生成 **TaskGraph**，核心是**根据并行度拆分 JobVertex 为 Task**。

#### （1）核心步骤

1. **并行度拆分**：每个 `JobVertex` 根据配置的并行度，拆分为多个`TaskVertex`（物理算子）。例如：并行度为 4 的 `Window-Aggregate` JobVertex → 拆分为 4 个 TaskVertex；

2. **确定数据传输模式**：根据 JobEdge 的类型（One-to-One / Shuffle），确定 Task 之间的数据传输方式：
        

    - **One-to-One**：上下游 Task 一一对应（如 Source 与 map），数据直接传输，无网络开销；

    - **Shuffle**：上下游 Task 多对多对应（如 keyBy 之后），需要通过网络进行数据重分区（如 Hash Shuffle）；

3. **资源分配绑定**：将 TaskVertex 分配到 TaskManager 的 Slot 中，生成最终可执行的 Task。

#### （2）量化场景示例

延续上文的行情处理 JobGraph：

- `Source-Map-Filter` JobVertex 并行度设为 4 → 拆分为 4 个 Task（Task1~Task4）；

- `Window-Aggregate` JobVertex 并行度设为 4 → 拆分为 4 个 Task（Task5~Task8）；

- 由于 `keyBy` 触发 Shuffle，Task1~Task4 的输出会根据股票代码（Symbol）的 Hash 值，发送到 Task5~Task8 中对应的 Task；

- 最终 TaskGraph 包含 **8 个 Task**，对应 8 个并行执行的实例。

### 2. TaskGraph 的核心结构

TaskGraph 的核心组成是 `TaskVertex`（物理算子）和 `IntermediateResultPartition`（数据分区），包含以下关键信息：

1. **TaskVertex**：每个 TaskVertex 对应一个并行的 Task，存储算子的物理执行信息（如并行度、资源需求、状态后端配置）；

2. **IntermediateResultPartition**：描述 Task 输出的数据分区，是 Task 之间数据传输的依据；

3. **调度信息**：Task 之间的依赖关系、Slot 分配策略、故障恢复策略。

### 3. 核心作用

- 是 JobMaster 调度 Task 的**直接依据**，决定了 Task 在哪个 TaskManager 的哪个 Slot 中运行；

- 明确了 Task 之间的数据传输方式，是影响作业性能的关键因素。

## 四、JobGraph 与 TaskGraph 核心差异对比

|**维度**|**JobGraph**|**TaskGraph**|
|---|---|---|
|生成方|Client|JobMaster|
|核心定位|逻辑执行计划（提交给集群）|物理执行计划（执行给 TaskManager）|
|核心优化|算子链合并（减少逻辑节点）|并行度拆分（生成物理 Task）|
|组成单元|JobVertex（算子链）、JobEdge|TaskVertex（物理算子）、IntermediateResultPartition|
|并行度信息|只存储并行度配置，未拆分|根据并行度拆分为多个 Task，明确并行实例|
|数据传输|标记是否需要重分区|明确数据传输模式（One-to-One/Shuffle）|
|作用范围|Client → JobManager 的提交载体|JobMaster → TaskManager 的调度载体|
## 五、量化场景实战优化

### 1. 算子链优化（JobGraph 层面）

- **合并短算子**：对于量化策略中的轻量级算子（如行情解析、过滤），保留默认算子链合并，减少网络开销；

- **拆分重算子**：对于耗时的算子（如复杂因子计算、机器学习模型推理），使用 `disableChaining()` 禁用算子链，让其独立占用 Slot 资源，避免阻塞上下游。

### 2. 并行度匹配（TaskGraph 层面）

- **Kafka Source 并行度**：必须**等于 Kafka 主题的分区数**，避免分区倾斜（如 4 个 Kafka 分区对应 4 个 Source Task）；

- **聚合算子并行度**：根据行情数据量调整，如股票数量较多时，将 `Window-Aggregate` 算子的并行度设为 8~16，提升计算吞吐。

### 3. 数据传输优化（TaskGraph 层面）

- 减少 `keyBy` 次数：`keyBy` 会触发 Shuffle，增加网络开销，量化策略中尽量合并多次 `keyBy` 为一次；

- 使用**本地性调度**：JobMaster 会优先将 Task 调度到数据所在的 TaskManager（如 Kafka 分区所在节点），减少跨节点数据传输。

## 六、调试与查看执行图

在 Flink Web UI（默认 8081 端口）中，可以直观查看 JobGraph 和 TaskGraph：

1. 提交作业后，进入作业详情页；

2. 点击 **“Visualization”** 标签：
        

    - **JobGraph**：展示合并后的算子链和数据流关系；

    - **TaskGraph**：展示拆分后的 Task 及并行实例，可查看每个 Task 的运行状态、吞吐量和延迟。
> （注：文档部分内容可能由 AI 生成）