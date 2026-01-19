# Flink 学习笔记

## 第1章 Flink 概述
### 1.1 Flink 是什么
Flink 是一款**分布式、高性能、高可用、准确的流处理框架**，主要用于处理无界（实时）和有界（批处理）数据流：  
- **真正流处理**： 以流为核心，批处理是流处理的特例
- **低延迟 + 高吞吐**：支持毫秒级处理，且能应对高并发
- **精准的状态管理**：支持 Exactly-Once 语义（数据只处理一次）
- **容错性强**：基于 checkpoint 实现故障恢复

**核心概念（新手必记）**

| 概念         | 通俗解释                                                              |
|------------|-------------------------------------------------------------------|
| DataStream | 无界数据流（实时数据，如用户点击、日志）                                              |
| DataSet    | 有界数据流（批处理，如历史订单表）                                                 |
| Operator   | 算子：对数据的处理操作（如map、filter、keyBy、window）                             |
| State      | 状态：Flink 存储的中间计算结果（如累计用户点击次数）                                     |
| Checkpoint | 检查点：定期保存状态，故障时恢复，保证数据不丢不重复                                        |
| Window     | 窗口：将无界流切分成有界快进行计算（如统计每5分钟的订单量）                                    |
| Time       | 时间语义：EventTime（事件产生时间）、Processing Time（处理时间）、Ingestion Time（接入时间） |

### 1.2 Flink 特点
#### 1.2.1 以流为核心的统一计算模型
Flink 最核心的设计理念是 **一切皆流** ：批处理是流处理的特例（有界流），流处理是无界流的持续处理
- 对比传统框架：
  - Spark Streaming 是 **微批** 模型（将流切分成小批次处理），本质还是批处理
  - Flink 是真正的流式计算，数据一来就计算，无需等待批次结束
- 实际价值：既能处理实时的无界数据（如电商实时订单、股票行情），也能处理离线的有界数据（如历史订单报表），一套框架覆盖实时 + 离线场景，降低技术栈复杂度

#### 1.2.2 精准的状态管理与容错机制
Flink 提供完善的状态管理能力，是实现复杂业务逻辑的核心，且容错机制能保证数据一致性
- 状态类型丰富：
  - Keyed State: 按 Key 分区的状态，如按用户ID存储消费金额
  - Operator State: 算子级状态，如 Kafka 消费的 Offset
  - Broadcast State: 广播状态，如 实时更新的风控规则
- 一致性语义保障
  - Exactly-Once（精确一次）: 通过 Checkpoint + 两阶段提交（2PC）实现，数据只处理一次，无丢失，无重复
  - At-Least-Once（至少一次）: 保底语义，适用于允许少量重复的场景

#### 1.2.3 灵活的时间语义与窗口机制
Flink 解决了流处理的 **乱序数据** 和 **时间对齐** 的核心痛点
- 三种时间语义：
  - Processing Time（处理时间）：算子处理数据的系统时间，简单但精度低；
  - Event Time（事件时间）：数据产生的真实时间（如日志的 timestamp），支持乱序数据处理；
  - Ingestion Time（接入时间）：数据进入 Flink 的时间，折中方案。
- Watermark（水位线）：
  - 为 Event Time 提供 **截止线** ，允许配置最大乱序时间 （如500ms），保证乱序数据也能准确计算
- 多样化窗口：
  - 滚动窗口（Tumbling）：无重叠，如每5分钟统计一次订单量
  - 滑动窗口（Sliding）：有重叠，如每1分钟统计过去5分钟的销量
  - 会话窗口（Session）：按用户会话划分，如用户30分钟无操作则结束会话
  - 全局窗口（Global）：自定义窗口触发规则
- 实际价值：应对实时场景中的乱序数据（如物流轨迹延迟、行情数据卡顿），保证计算结果的准确性

#### 1.2.4 高吞吐、低延迟、高可用
Flink 从架构设计上保证的高性能和稳定性
- 高吞吐：基于流水线的数据处理（算子链化）、异步IO、增量 Checkpoint 等优化，单机可支撑百万级/秒的数据处理
- 低延迟：真正的流处理模型，无需等待批次，端到端延迟可达毫秒级
- 高可用：
  - JobManager 主备切换：避免单点故障
  - TaskManager 故障恢复：从 Checkpoint 恢复状态，不影响整体作业
  - 集群弹性扩缩容：支持基于 Yarn/K8S 动态调整资源
- 实际价值：支持高并发实时场景（如双十一订单实时计算，直播弹幕实时分析）

### 1.3 Flink的应用场景

#### 1.3.1 电商和市场营销

如：实时数据报表、广告投放、实时推荐

#### 1.3.2 物流网（IOI）

如：传感器实时数据采集和显示、实时报警、交通运输业

#### 1.3.3 物流配送和服务业

如：订单状态实时更新、通知信息推送

#### 1.3.4 银行和金融业

如：实时结算和通知推送、实时检测异常行为



### 1.4 Flink 功能特点（应用层面）

#### 1.4.1 丰富的API与生态集成

- **多层API与生态集成**
  - 低层 ProcessFunction：最灵活，可访问状态、时间、Watermark，适合复杂业务
  - 中层 DataStream/DataSet API：面相开发者的核心API，支持Java/Scala/Python
  - 高层 Table API/SQL：声明式API，降低开发门槛，支持SQL直接处理流数据
- 生态集成完善：
  - 数据源：Kafka、Pulsar、RabbitMQ、MySQL、HDFS、Elasticsearch等
  - 计算框架：与Spark、Hive、Flink CDS（实时同步数据库）无缝集成
  - 部署方式：Standalone、Yarn、K8s、Mesos，支持本地调试和生产集群部署

#### 1.4.2 轻量级部署与运维

- 无外部依赖（除JDK），单机即可启动集群
- Web UI可视化：支持作业监控、状态查看、故障定位、Checkpoint管理
- Savepoint机制：手动触发状态快照，支持作业升级、扩缩容时无数据丢失

#### 1.4.3 支持复杂事件处理（CEP）

Flink CEP 提供模式匹配能力，可识别数据流中的复杂事件序列

- 如 “用户5分钟连续3次登录失败” -> 触发风控告警
- 如 “股票价格10分钟上涨5%且成交量放大” -> 触发交易信号

#### 1.4.4 Flink 特点对比

| 特性        | Flink        | Spark Streaming   | Storm         |
|-----------|--------------|-------------------|---------------|
| 核心模型      | 纯流处理（批为特例）   | 微批处理              | 纯流处理          |
| 延迟        | 毫秒级          | 秒级                | 毫秒级           |
| 吞吐        | 高            | 高                 | 低             |
| 一致性语义     | Exactly-Once | Exactly-Once（复杂）  | At-Least-Once |
| 状态管理      | 完善（多类型）      | 基础（仅 Keyed State） | 弱             |
| 时间语义 / 窗口 | 丰富           | 基础                | 简单            |
| 易用性       | 中（API 丰富）    | 高（SQL/DSL）        | 低（需手动开发）      |



## 第2章 Flink 快速上手



### 2.1 第一个 Flink 程序（实战入门）

我们用 Java 写一个简单的**实时单词计数**程序（最经典的入门案例），核心是读取 Socket 实时数据，统计每个单词的出现次数

#### 2.1.1 Maven 依赖（pom.xml）

```java
<dependencies>
    <!-- Flink 核心依赖 -->
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-java</artifactId>
        <version>1.17.0</version>
    </dependency>
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-streaming-java</artifactId>
        <version>1.17.0</version>
    </dependency>
    <!-- Flink 客户端依赖（本地运行用） -->
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-clients</artifactId>
        <version>1.17.0</version>
    </dependency>
</dependencies>
```

#### 2.1.2 代码实现1 DataSet API

```java
# 批处理

public class WordCountBatchStream {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 2. 读取数据
        String inputPath = "E:\\Idea\\Idea_Study\\Flink\\chapter02-introduction\\input\\word.txt";
        DataSource<String> dataSource = env.readTextFile(inputPath);
        // 3. 将读取数据进行扁平化处理，封装为二元组对象 Tuple2<单词，1L>
        FlatMapOperator<String, Tuple2<String, Integer>> wordOneOperator = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String lineVal, Collector<Tuple2<String, Integer>> collector) throws Exception {
                // 分割字符串数据
                String[] words = lineVal.split(" ");
                for (String word : words) {
                    Tuple2<String, Integer> tuple2 = Tuple2.of(word, 1);
                    collector.collect(tuple2);
                }
            }
        });
        // 4. 按照单词进行分组
        UnsortedGrouping<Tuple2<String, Integer>> group = wordOneOperator.groupBy(0);
        // 5. 聚合计算
        AggregateOperator<Tuple2<String, Integer>> sum = group.sum(1);
        // 6. 打印结果
        sum.print();
    }

}

```
#### 2.1.3 代码实现2 DataStream API

```java
# 流处理

public class WordCountStream {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境（本地环境，集群运行时会自动配置）
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度（本地运行方便查看结果）
        env.setParallelism(1);

        // 2. 读取数据
        String inputPath = "E:\\Idea\\Idea_Study\\Flink\\chapter02-introduction\\input\\word.txt";
        DataStreamSource<String> fileStream = env.readTextFile(inputPath);
        // 3. 数据处理: 切分单词 -> 组装元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordOneOperator = fileStream
                .flatMap((String lineVal, Collector<Tuple2<String, Integer>> collator) -> {
                    String[] words = lineVal.split(" ");
                    for (String word : words) {
                        Tuple2<String, Integer> tuple2 = Tuple2.of(word, 1);
                        collator.collect(tuple2);
                    }
        }).returns(new TypeHint<Tuple2<String, Integer>>() {});
        // 4. 按单词分组
        KeyedStream<Tuple2<String, Integer>, String> wordKeyedStream = wordOneOperator.keyBy(e -> e.f0);
        // 5. 按分组统计
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = wordKeyedStream.sum(1);
        // 6. 输出
        sum.print();
        // 7. 执行 如果使用的是 DataStream API, 需要通过 env.execute()显式提交作业
        env.execute();
    }

}
```
#### 2.1.4 代码实现3 DataStream API

```JAVA
/**
 * 无界流处理
 */
public class WordCountUnBoundStream {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 读取数据
        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 9999);
        // 3. 数据处理
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordOneOperator = dataStream
                .flatMap((String lineVal, Collector<Tuple2<String, Integer>> collector) -> {
                    String[] words = lineVal.split(" ");
                    for (String word : words) {}
                }).returns(new TypeHint<Tuple2<String, Integer>>() {});
        // 4. 分组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordOneOperator.keyBy(e -> e.f0);
        // 5. 聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyedStream.sum(1);
        sum.print();
        env.execute();
    }
}

```

#### 2.1.5 链式调用泛型擦除问题

```JAVA
public class WordCount_04_StreamGeneric {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境（本地环境，集群运行时会自动配置）
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度（本地运行方便查看结果）
        env.setParallelism(1);

        // 2. 读取数据
        String inputPath = "E:\\Idea\\Idea_Study\\Flink\\chapter02-introduction\\input\\word.txt";
        DataStreamSource<String> fileStream = env.readTextFile(inputPath);
        // 3. 数据处理: 切分单词 -> 组装元组
        fileStream.flatMap((String lineVal, Collector<Tuple2<String, Integer>> collator) -> {
            String[] words = lineVal.split(" ");
            for (String word : words) {
                Tuple2<String, Integer> tuple2 = Tuple2.of(word, 1);
                collator.collect(tuple2);
            }
        })
//                .returns(new TypeHint<Tuple2<String, Integer>>() {})
                // 注意: 在使用 lamba 表达式时，因为泛型擦除问题，需要显式的指定返回的类型
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(e -> e.f0)
                .sum(1)
                .print();
        // 7. 执行
        env.execute();
    }

}
```



## 第3章 Flink部署

### 3.1 集群角色

#### 3.1.1 集群核心角色总览

Flink 集群采用**主从架构**，核心角色分为三大类：**Client（客户端）、JobManager（主节点）、TaskManager（从节点）**。每个角色有明确的分工，共同支撑分布式作业的提交、调度、执行和容错

#### 3.1.2 核心角色深度解析

##### 3.1.2.1 Client （客户端）

- **定位**：用于与 Flink 集群的交互入口，非集群常驻节点（作业提交后可退出）

- **核心职责**：

  1. **作业编译与优化**：将用户编写的 Flink 代码（DataStream、Table API）编译成  JobGraph（作业的逻辑执行图），并进行基础优化（如算子链合并）
  2. **作业提交**：将 JobGraph 和依赖包提交给 JobManager，同时可指定并行度、状态后端等配置
  3. **辅助功能**：提交作业启停、状态查询、日志查看等运维命令（如 flink run / flink stop）

- **典型交互场景**：

  ```bash
  # Client 提交作业的命令 （底层就是 Client 与 JobManager 交互）
  flink run -d -p 4 /opt/quant-strategy.jar
  ```

  (-d: 后台运行，-p 4：指定并行度，Client 会把这些参数传递给 JobManger)

##### 3.1.2.2 JobManager（主节点）

- **定位**：集群的**大脑**，负责集群**资源管理、作业调度、容错控制**，是集群常驻核心节点（生产环境通常部署1主1备，避免单点故障）

- **内部细分角色 （JobManager 的核心组件）**

  | 子组件           | 核心职责                                                     | 量化场景示例                                                 |
  | ---------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
  | ResourceManageer | 集群资源统一管理：<br />1. 向 Yarn/K8s 申请 TaskManager 资源；<br />2. 为作业分配 Slot（资源单元）<br />3. 回收空闲资源 | 量化策略提交后，ResourceManageer 根据并行度=4的要求，未作业分配 4 个 Slot |
  | Dispatcher       | 作业接入与分发：<br />1. 接受 Client 提交的作业<br />2. 启动 JobMaster（单作业的管理者）<br />3. 提供 Web UI 和 REST API | 接收量化行情统计作业，启动对应的 JobMaster，并在 Web UI（8081）展示作业状态 |
  | JobMaster        | 但作业全生命周期管理：<br />1. 将 JobGraph 转换为 TaskGraph（物理执行图）<br />2. 调度 Task 到 TaskManager 的 Slot 中执行<br />3. 协调 Checkpoint（状态快照）<br />4. 作业故障恢复（重启失败 Task） | 行情计算作业中，若某个 Task 宕机，JobMaster 会重新调度该 Task 到其他 TaskManager |

  

- **关键特性**：

  **主备机制**：生产环境部署多个 JobMaster，通过 Zookeeper 选举 Leader，Leader 负责所有核心操作，Follower 仅在 Leader 宕机时接管，保证集群高可用

##### 3.1.2.3 TaskManager（从节点）

- **定位**：集群的**手脚**，负责执行具体的计算任务，是实际处理数据的节点（可部署多个，数量决定集群的计算能力）
- 核心职责
  1. **资源提供**：每个 TaskManager 启动时会创建多个 Slot（默认1个，可配置），Slot 是 Flink 的最小资源单元（包括CPU、内存），用于运行 Task
  2. **Task执行**：接受 JobMaster 的调度命令，启动 Task （算子的并行实例），执行数据处理逻辑（如行情解析、涨幅计算）
  3. **状态管理**：存储和维护 Task 的状态（如股票最新价），并配合 Jobmaster 完成 Checkpoint
  4. **数据传输**：与其他 TaskManager 进行数据交互（如 Shuffle、双流 Join）
- **核心概念**：**Slot 与 Task**
  - 1 个 Slot 可运行多个 Task（基于**算子链**优化），例如：数据源 -> map -> filter 三个算子可链化为 1 个 Task，运行在 1 个 Slot 中，减少数据传输开销
  - 量化场景配置建议：TaskManger 的 Slot 数 = CPU 核心数（如8核机器配置8个Slot），充分利用硬件资源



#### 3.1.3 角色协作流程（量化行情统计作业为例）

```tex
Client → Dispatcher(JobManager) → JobMaster → ResourceManager(JobManager)
                                                          ↓
                    ┌───────────────────┬──────────────────┘
                    │                   │
            TaskManager1(Slot1/2)    TaskManager2(Slot1/2)
                ↓                         ↓
        处理股票A行情               统计1分钟涨幅
                └───────────────────┬──────────────────┘
                                    ↓
                                 JobMaster
```

1. Client 将量化行情统计作业编译为 JobGraph，提交给 JobManager 的 Dispatcher
2. Dispatcher 启动该作业的 JobMaster
3. JobMaster 向 ResourceManager 申请 4 个 Slot（因并行度为 4）
4. ResourceManager 从集群的 TaskManager 中分配 4 个 Slot（如 TaskManager1 分配 2 个，TaskManager2 分配 2 个）
5. JobMaster 将 JobGraph 转化为 TaskGraph，调度 Task 到分配的 Slot 执行
6. TaskManager 执行具体的行情解析、涨跌幅计算任务，并定期向 JobMaster 上报状态，配合完成 Checkpoint；
7. 若某个 Task 势必，JobMaster 会重新调度该 Task 到其他空闲 Slot

### 3.2 Flink 集群搭建



#### 3.2.1 集群启动



#### 3.2.2 向集群提交作业



### 3.3 部署模式



### 3.4 Standalone 运行模式 （了解）



### 3.5 YARN 运行模式 （重点）



### 3.6 K8S 运行模式 (了解)



### 3.7 历史服务器



## 第4章 Flink 运行时架构

### 4.1 系统架构

#### 4.1.1 架构图

![Flink 运行时架构 - Stanalone会话模式为例](E:\Idea\Idea_Study\Flink\Flink 运行时架构 - Stanalone会话模式为例.jpg)

先明确几个基础概念：

- **作业（Job）**：用户编写的 Flink 程序（如读取 Kafka 数据 -> 处理 -> 写入MySQL）提交后，会被转化为一个Jog
- **任务（Task）**：Job 会被拆解为多个并行执行的最小计算单元，就是 Task（由算子 Operator 并行化而来）
- **并行度（Parallelism）**：同一算子的 Task 数量，决定了该算子的并发处理能力
- **任务槽（Task Slot）**：Flink TaskManager 的资源单位，每个 Slot 对应一组固定的 CPU/内存资源，一个 Slot 可以运行多个 Task（但属于同一Job）

#### 4.1.2 Flink 作业执行流程（从提交到运行）

1. **Client 提交作业**：用户编写的 Flink 代码（如 DataStream 程序）通过 Client 编译为 JobGraph，提交给 Dispatcher。
2. **创建 JobMaster**：Dispatcher 启动对应的 JobMaster，JobMaster 将 JobGraph 转换为 ExecutionGraph（并行化）。
3. **申请资源**：JobMaster 向 ResourceManager 申请 Slot 资源。
4. **分配 Slot**：ResourceManager 向空闲的 TaskManager 请求 Slot，TaskManager 分配 Slot 后反馈给 JobMaster。
5. **调度 Task**：JobMaster 将 Task 调度到分配好的 Slot 中执行。
6. **执行计算**：TaskManager 中的 Task 处理数据，Task 之间通过数据流（Stream）传输数据，同时定期生成 Checkpoint 保证容错。
7. **作业结束 / 容错**：若作业正常完成，释放资源；若 Task 失败，JobMaster 重新调度 Task 到其他 Slot 执行（基于 Checkpoint 恢复状态）

#### 4.1.3 JobManager（作业管理器）

JobManager 是一个 Flink 集群中任务管理和调度的核心，是控制应用执行的主进程。也就是说，每个应用都应该被唯一的 JobManager 所控制执行

JobManager 是集群的**控制中心**，核心由3个子模块组成

| 子模块                        | 概述                                                         | 核心职责                                                     |
| ----------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| JobMaster                     | 1. JobMaster 是 JobManager 中最核心的组件，**负责处理单独的作业（Job）**。所以 JobMaster 和具体的 Job 是一一对应的，多个 Job 可以同时运行在一个 Flink 集群中，每个Job 都由一个自己的 JobMaster。<br />2. 在作业提交时，JobMaster 会先接收要执行的应用。JobMaster 会把 JobGraph 转换为一个物理层面的数据流图，这个图被叫做**执行图（ExecutionGraph）**，它包含了所有可以并发执行的任务。JobMaster 会向资源管理器（ResourceManager）发出请求，申请执行任务必要的资源，一旦它获取到了足够的资源，就会将执行图发到真正运行他们的 TaskManager<br />3. 在运行过程中，JobMaster 会负责把所有需要中央协调的操作，比如检查点（Checkpoint）的协调 | 单个Job的**专属管家**：<br />1. 接收 Client 提交的 JobGraph，生成ExecutionGraph<br />2. 向 ResourceManager 申请 Slot<br />3. 调度 Task 到 TaskManager<br />4. 监控 Task 执行状态，处理故障恢复<br />5. 暴露 Job 的metrics和状态查询接口 |
| 资源管理器（ResourceManager） | ResourceManager 主要**负责资源的分配和管理**，在 Flink集群中只有一个。所谓**资源**，主要是指 TaskManager 的任务槽（task slots）。任务槽就是 Flink 集群中的资源调配单元，包含了机器用来执行计算的一组CPU和内存资源。每一个任务（Task）都需要分配到一个 slot 上执行 | 集群级资源管理者：<br />1. 管理所有 TaskManager 的 Slot 资源池<br />2. 响应 JobMaster 的 Slot 申请<br />3. 当集群资源不足时，向外部集群（YARN/K8s）申请新的 TaskManager<br />4. 回收空闲的 Slot/TaskManager资源 |
| 分发器（Dispatcher）          | Dispatcher 主要提供一个 REST 接口，**用来提交应用，并且负责为每一个新提交的作业启动一个新的 JobMaster 组件**。Dispatcher 也会启动一个 Web UI，用来方便展示和监控作业执行的信息。Dispatcher 在架构中并不是必须的，在不同的部署模式下可能被忽略掉 | 集群入口网关：<br />1. 接收 Client 的作业提交请求，分配 JobMaster<br />2. 提供 Web UI，展示集群和作业状态<br />3. 管理 Session 模式下的 JobMaster 生命周期 |

> **关键细节**：**每个 Job 对应一个独立的 JobMaster，即使一个 Job 失败，也不会影响集群中其他 Job 的运行，提升了集群的隔离性**

#### 4.1.3 TaskManager（任务管理器）

TaskManager 是真正执行计算的**工作节点**，核心结构：

- **Slot 池（Slot Pool）**：

  - 每个 TaskManager 启动时，会根据配置（taskmanager.numberOfTaskSlots）创建固定数量的 Slot
  - 每个 Slot 是独立的资源单元（内存+CPU隔离），默认情况下，Slot 会共享 TaskManager 的 JVM 堆外内存和网路资源

- **TaskExecutor**：

  - Slot 的 **执行者**，负责启动/停止 Task
  - 与 JobMaster 通信，上报 Slot 状态、接受 Task 调度指令、上报 Task 执行状态
  - 与其他 TaskExecutor 通信：通过 Netty 实现 Task 间的数据传输（如Shuffle、广播）

- **内存模型**：TaskManager 的内存被严格划分，避免OOM，核心分区

  - Heap Memory：JVM 对内存，存储用户代码的对象、算子状态（如 State Backend 用堆内存时）
  - Off-Heap Memory：堆外内存，存储网络缓冲区、Shuffle数据、RocksDB状态（推荐生产使用）
  - ManagedMemory：管理内存，用于排序、哈希表、RocksDB缓存，可动态调整

- **I/O管理器**：

  负责 Task 的临时文件管理（如 Shuffle 时的临时数据，Checkpoint的数据落地），保证数据读写的高效性

### 4.2 核心概念

#### 4.2.1 并行度（Parallelism）





#### 4.2.2 算子链（Operator Chain）





####  4.2.3 任务槽和并行度的关系

