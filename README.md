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

##### 4.2.1.1 核心概念

​	Flink 的并行度是指**一个算子（Operator）、算子子任务（Subtask）或整个作业（Job）同时执行的实例数量**。简单来说，并行度决定了 Flink 任务能利用多少计算资源（如 CPU 核心）来处理数据，是 Flink 实现分布式计算的核心配置

​	例如：当前要处理的数据量非常大时，我们可以把一个算子操作，“复制” 多份到多个节点，数据来了之后就可以到其中任意一个执行。这样一来，一个算子任务就被拆分成了多个并行的 “子任务” （subtasks），再将它们分发到不同的节点，真正实现了并行计算

​	再 Flink 执行过程中，每一个算子（operator）可以包含一个或多个子任务（operator subtask），这些子任务在不同的线程、不同的物理机或不同的容器中完全独立地执行



​	一个特定算子的<span style="color:red">**子任务（subtask）的个数**</span>被称之为<span style="color:red">**并行度（parallelism）**</span>。包含并行子任务的数据流，就是<span style="color:red">**并行数据流**</span>，它需要多个分区（stream partition）来分配并行任务。一般情况下，<span style="color:red">**一个流程序的并行度**</span>，可以认为就是<span style="color:red">**其所有算子中最大的并行度**</span>。一个程序中，不同的算子可能有不同的并行度

##### 4.2.1.2 并行度的层级与优先级

Flink 中并行度可以在 4 个层级配置，优先级从高到低一次是：

1. **算子级别（最高）**：为单个算子指定并行度
2. **执行环境级别**：为当前作业的所有算子设置默认并行度
3. **客户端级别**：提交作业时通过命令执行
4. **集群级别（最低）**：Flink 集群的默认配置（flink-conf.yaml 中的 parallelism.default）

**示例：不同层级的并行度设置**

```java

public class Parallelism_Demo {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 执行环境级别：设置当前作业默认并行度为 2
        env.setParallelism(2);

        // 3. 算子级别：为 source 算子设置并行度 3（优先级高于环境级别）
        env.addSource(new CustomSource())
                .setParallelism(1)  // Source 并行度 3 这是设置为3会报错
                .map(value -> value + " processed")
                .setParallelism(4)  // Map 并行度 4
                .print()            // Print 算子使用环境默认并行度 2
                .setParallelism(2);

        // 执行作业
        env.execute();
    }

    // 自定义简单 source
    static class CustomSource implements SourceFunction<String> {
        private boolean isRuning = true;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            int i = 0;
            while (isRuning) {
                ctx.collect("data-" + i++);
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            isRuning = false;
        }
    }

}

```



#### 4.2.2 算子链（Operator Chain）

![image-20260121221616574](E:\Idea\Idea_Study\Flink\image-20260121221616574.png)

一个数据流在算子之间传输数据的形式可以是一对一（one-to-one）的直通（forwarding）模式，也可以是打乱的重分区（redistributing）模式，具体是哪一种取决于算子的种类

#####  4.2.2.1 为什么需要啊算子链

Flink 作业执行时，算子之间的数据传输有两种格式：

1. **同一个Task内**：算子间数据直接在 JVM 内存中传递，无需序列化、网络IO、跨线程切换，效率极高

2. **不同Task间**：数据需要经过序列化、通过网络（或本地管道）传输、在反序列化，还有任务调度的开销，性能的损耗

   算子链的核心价值就是**减少跨Task的数据传输开销**，提升作业整体吞吐和降低延迟，这是 Flink 的默认优化策略（无需手动配置即可生效）

##### 4.2.2.2 算子链的合并条件

不是所有的算子都能合并成一个链，必须满足以下条件（核心条件）

1. **上下游算子的并行度相同**：并行度不同的算子无法合并（比如上游并行度 5， 下游并行度 10， 必须要拆分 Task）
2. **上下游算子之间是一对一（One-to-One）的数据流关系**：这种关系也叫**转发流（Forward Stream）**，即上游算子的输出直接转发给下游算子，数据不会被重分区（Shuffle）
   - 符合的场景：map()、flatMap()、filter() 等无数据重分区的算子串联
   - 不符合的场景：keyBy()、rebalance()、window() 等会触发数据重分区/Shuffle的操作，这些操作会打断链子
3. **算子都在同一个执行环境（Execution Environment）中**，且属于同一个作业
4. **算子的链策略（Chaining Strategy）允许合并**：每个算子都有对应的链策略，默认是允许合并
5. **作业的整体并行执行模式（Exexction Mode）不是批处理的 "严格批处理模式"**（特殊场景，新手暂无需深入）

##### 4.2.2.3 合并算子链

​	在 Flink 中，**并行度相同的一对一（One-to-One）算子操作，可以直接链接在一起形成一个 “大” 的任务（Task）**，这样原来的算子就成为了真正任务里的一部分，如下图所示。每个 task 会被一个线程执行。这样的技术被称为 “算子链（Operator Chain）”

##### 4.2.2.4 直观示例：算子链的表现

比如下面这段简单的 Flink 代码

```JAVA
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class OperatorChainingDemo {
    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 保持默认并行度（通常为CPU核心数），便于观察算子链
        env.setParallelism(2);

        // 2. 构建数据流：source -> map -> filter -> print
        env.addSource(new CustomSource())
                .map(value -> "map处理后：" + value) // 算子1
                .filter(value -> value.length() > 10) // 算子2
                .print(); // 算子3

        // 3. 执行作业
        env.execute("Operator Chaining Demo");
    }

    // 自定义简单Source
    static class CustomSource implements SourceFunction<String> {
        private boolean isRunning = true;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            int count = 0;
            while (isRunning) {
                ctx.collect("数据" + count++);
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
```

在 Flink Web UI 中查看这个作业的执行图（Job Graph），你会发现：

- CustomSource、map、filter、print 会被合并成同一个 Task (因为满足所有合并条件)
- 由于并行度设置为 2， 会生成 2 个完全相同的 Task 实例，各自独立处理一部分数据

如果在代码中加入 keyBy() （会触发 Shuffle）比如：

```JAVA
env.addSource(new CustomSource())
        .map(value -> "map处理后：" + value)
        .keyBy(value -> value.length()) // 插入keyBy，打断算子链
        .filter(value -> value.length() > 10)
        .print();
```

此时的算子链会被拆分成两个 Task：

1. Task1: CustomSource + map
2. Task2：filter + print

​	两者之间通过 Shuffle 传输数据，无法合并

##### 4.2.2.5 算子链的禁用

Flink 允许手动干预算子链的合并，满足特殊场景需求（比如调试、资源隔离），核心由3中操作

1. **全局禁用算子链（整个作业）**

   禁用后，所有算子都不会合并，每个算子都是一个独立的 Task，适合调试时排查问题

   ```JAVA
   // 获取执行环境后，调用该方法禁用全局算子链
   env.disableOperatorChaining();
   ```

   

2. **局部打断算子链（某个算子之后）**

   在指定算子之后添加 startNewChain()，表示该算子之后的算子会开启一个新的算子链，当前算子与之前的算子仍会合并（局部拆分）

   ```JAVA
   env.addSource(new CustomSouorce())
       .map(value -> "map处理之后：" + value)
       .startNewChain() // map之后的算子开启新链
       .filter(value -> value.length() > 10)
       .print();
   ```

   此时会生成两个 Task：

   - Task1：CustomSource + map
   - Task2：filter + print

3. **单个算子禁用合并（不参与任何算子链）**

   给指定算子添加 disableChaining()，表示该算子独立成一个 Task，既不与前面的算子合并，也不与后面的算子合并

   ```JAVA
   env.addSource(new CustomSource())
       .map(value -> "map处理后" + value)
       .disableChining() // map 算子独立成 Task
       .filter(value -> value.length() > 10)
       .print()
   ```

   此时会生成三个 Task:

   - Task1：CustomSource
   - Task2：map（独立）
   - Task3：filter + print

##### 4.2.2.6 补充：算子链的底层本质

1. 合并后的算子链，在执行时对应一个**SubTask(Flink 的最小执行单元)**，运行在一个独立的线程
2. 算子链内部的算子，按照数据流顺序一次执行，数据采用 “流水线” 方式传递，无额外开销
3. 算子链不会改变作业的业务逻辑，仅改变作业的执行拓扑（Task划分），是纯性能优化手段

#### 4.2.3 任务槽（Task Slots）

​	Flink 中每一个 TaskManager 都是一个 JVM 进程，它可以启动多个独立的线程，来并行执行多个子任务（subtask）

​	很显然，TaskManager 的计算资源是有限的，并行的任务越多，每个线程的资源就会越少。那一个 TaskManager 到底能并行执行多少个任务呢？为了控制并发量，我们需要再 TaskManager 上对每个任务运行所占用的资源做出明确的划分，这就是所谓的<span style="color:red">**任务槽（Task Slots）**</span>

​	每个任务槽（task slot）其实表示了 TaskManager 拥有计算资源的一个固定大小的子集。这些资源就是用来独立执行一个子任务的



​	假设一个 TaskManager 有三个 slot ，那么他会将管理的内存平均分成三份，每个 slot 独自占一份。这样一来，我们**在 slot 上执行一个子任务时，相当于划定了一块内存 “专款专用”**，就**不需要跟来自其他作业的任务去竞争内存资源了**

​	所以现在我们**只要2个TaskManager**，就可以并行处理配好的 5 个任务了

![image-20260122001758055](E:\Idea\Idea_Study\Flink\image-20260122001758055.png)

##### 4.2.3.1 任务槽是什么

**Task Slot 是 Flink 为 TaskManager 划分的最小资源单位**，可以把它理解成 TaskManager （Flink的工作节点），上的 “资源插槽”。每个 Task Slot 会独占 TaskManager 的一部分硬件资源（内存为主，CPU由TaskManager共享），用于运行任务

**核心背景**：

- 一个 TaskManager 是一个 JVM 进程，默认情况下，Flink 会给每个 TaskManager 分配 1 个 Slot，你也可以通过配置调整 Slot 数量（比如设置为4、8等）
- 每个 Slot 对应 TaskManager 中固定比例的内存资源（总内存/Slot数），CPU则由所有 Slot 共享（Flink 1.10+ 支持CPU隔离，需结合YARN/K8s等资源管理器）

##### 4.2.3.2 任务槽的作用

1. **资源隔离**

   Task Slot 最核心的作用是**内存隔离**

   - 不同 Slot 中的任务不会共享 JVM 堆内存，避免一个任务的内存溢出影响其他任务
   - 但同一 TaskManager 下的所有 Slot 会共享 JVM 的元空间（Metaspance）、线程池，减少资源开销

2. **控制并行度**

   Task Slot 的数量直接决定了 Flink 任务的**最大并行上限**：

   - 整个集群的总 Slot 数 = 所有 TaskManager 的 Slot 数之和
   - 任务的并行度（Parallelism）不能超过集群总 Slot 数（否则任务无法全部启动）

3. **任务链（Operator Chain）优化**

   Flink 会将上下游无数据 shuffle 的算子（比如 map + filter）合并成一个**任务链（Task Chain）**，一个任务链会运行在一个 Slot 中，减少线程切换和数据传输开销

   - 示例：一个包含 Source -> map -> filter -> sink 的无 shuffle 作业，即使并行度为 4， 也只会占用 4 个 Slot （而非16个）

##### 4.2.3.3 任务槽的配置与使用

1. **配置 TaskManager 的 Slot 数**

   有 3 中常见的配置方式：

   **（1）配置文件（flink-conf.yaml）**

   ```yam
   # 每个 TaskManager 的默认 Slot 数（全局配置）
   taskmanager.numberOfTaskSlots: 4
   ```

   **(2) 启动 TaskManager 时指定（命令行）**

   ```BASH
   # 启动单个 TaskManager 指定 Slot 数为 8
   ./bin/taskmanager.sh start --slot 8
   ```

   **(3) 提交作业时指定（覆盖全局配置）**

   ```BA
   # 提交作业时，指定该作业使用的 Slot 数（实际是作业并行度）
   ./bin/flink run -p 4 /path/to/your/job.jar
   ```

2. **关键示例：Slot 与并行度关系**

   假设你有 2 个TaskManager，每个配置 3 个 Slot

   - 集群总 Slot 数 = 2 * 3 = 6
   - 如果你提交的作业并行度设置为 4
     - 只会占用 4 个 Slot，剩余 2 个可运行其他作业
     - 每个并行子任务（Subtask）会分配到一个独立的 Slot 中
   - 如果你提交的作业并行度设置为 8
     - 作业会处于 “待调度” 状态，直到集群新增 2 个 Slot （比如启动新的 TaskManager）

##### 4.2.3.4 任务槽的常见误区

1. **误区1：** 1个 Slot = 1 个 CPU 核心
   - 错误：默认情况下 Slot 只隔离内存，CPU 是共享的。CPU 隔离需要结合资源管理器（如YARN配置 yarn.containers.vcores）
2. **误区2：**：Slot 数越多越好
   - 错误：Slot 数过多会导致单个 TaskManager 的内存被过度拆分（每个 Slot 内存过小），容易触发 OOM；Slot 数过少则会浪费资源
   - 建议：根据 TaskManager 的内存大小配置 Slot 数（比如 16G 内存的 TaskManager，配置 4 个 Slot，每个 Slot 分配 4G 内存）
3. **误区3：**作业并行度必须等于 Slot 数
   - 错误：并行度可以小于 Slot 数（资源富裕），也可以通过动态扩容 Slot 来满足更大的并行度需求



####  4.2.4 任务槽和并行度的关系

​	**任务槽和并行度都跟程序的并行执行有关，但两者是完全不同的概念**。简单来说**任务槽是静态的概念**，是指 TaskManager 具有的并发执行能力，可以通过参数进行配置；而并行度是动态的概念，也就是 TaskManager 运行程序时实际使用的并发能力

​	举例说明：standalone 的会话模式，假设一共有 3 个 TaskManager，每一个 TaskManager 中的 slot 数量设置为 3 个，那么一共有 9 个 task slot，表示集群最多能并行执行 9 个统一算子的任务

​	而我们定义 word count 程序的处理操作是四个转换算子

​	source -> flatmap -> reduce -> sink

​	当所有算子并行度相同时，容易看出 source 和 flatmap 可以合并为 算子链，于是最终有三个任务节点



## 第5章 DataStream API

![image-20260124232436426](E:\Idea\Idea_Study\Flink\image-2026012423243642644.png)

### 5.1 执行环境（Execution Environment）

#### 5.1.1 核心概念

Flink 执行环境（ExecutionEnvironment 或 StreamExecutionEnvironment）的核心作用可以概括为3点：

1. **初始化上下文**：为 Flink 应用创建一个运行上下文、加载配置（并行度、状态后端等），关联底层集群资源（本地、YARN、K8s等）
2. **创建数据源**：通过执行环境提供的 API 读取外部数据（文件、Kafka、MySQL等），生成 Flink 最核心的 DataSet（批处理）或 DataStream（流处理）
3. **提交执行任务**：触发应用的执行（调用 execute()），将业务逻辑转换成 Flink 的 JobGraph，提交给集群（或本地）运行

注意：Flink 1.12 之后引入了**统一流批处理(Unified Btach & Stream Procession)**，推荐使用 StreamExecutionEnvironment 同时支持流处理和批处理（通过配置执行模式切换），传统的 ExecutionEnvironment（仅批处理）逐渐被弱化

#### 5.1.2 执行环境的 3 种核心分类

根据运行部署的环境不同，Flink 提供了 3 种常用执行环境创建方式，满足开发测试、生产部署等不同场景

1. **本地执行环境（Local Execution Environment）**

   用于**本地开发、调试和单元测试**，不需要搭建 Flink 集群，直接在当前 JVM 中运行任务，资源可控

   **核心创建方式：**

   ```JAVA
   public class LocalEnvDemo {
   
       public static void main(String[] args) throws Exception {
           // 方式1: 创建默认本地之心环境（并行度默认等于当前机器的 CPU 核心数）
           LocalStreamEnvironment env1 = StreamExecutionEnvironment.createLocalEnvironment();
           
           // 方式2: 指定并行度的本地执行环境（推荐，调试时并行度可控，避免混乱）
           LocalStreamEnvironment env2 = StreamExecutionEnvironment.createLocalEnvironment(2);
           
           // 方式3: 带配置的本地执行环境（可配置更多参数，如状态后端、内存等）
   //        Configuration conf = new Configuration();
   //        conf.setInteger("state.backend.rocksdb.memory.off-heap", 1);
   //        LocalStreamEnvironment env3 = StreamExecutionEnvironment.createLocalEnvironment(2, conf);
           
           // 后续业务逻辑（示例：读取本地文件）
           env1.readTextFile("file:///tmp/test.txt")
                   .print();
           
           // 提交作业（本地环境，execute()）会直接触发运行
           env2.execute("Local Flink Job Demo");
       }
   
   }
   ```

   **关键特性**：

   - 无需集群，开箱即用，适合开发阶段
   - 支持断点调试，能直观看到任务运行的结果
   - 资源有限（受当前机器限制），不适合大规模数据测试

2. **集群执行环境（Cluster Execution Environment）**

   用于**生产环境配置**，任务会提交到已搭建好的 Flink 集群（Standalone、YARN、K8s等）运行，充分利用集群资源

   **核心创建方式**

   集群环境不需要手动指定**集群地址**，而是通过 getExecutionEnvironment() 自动适配，这也是**生产环境的推荐写法**（保证代码的可移植性，无需修改代码即可在不同环境运行）

   ```java
   import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
   
   public class ClusterEnvDemo {
       public static void main(String[] args) throws Exception {
           // 核心方法：自动获取执行环境
           // 规则：
           // 1. 若本地运行（无集群提交），等价于 getLocalEnvironment()
           // 2. 若通过 flink run 提交到集群，等价于集群执行环境
           StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
           
           // 配置全局并行度（可选，可被集群配置覆盖）
           env.setParallelism(4);
           
           // 后续业务逻辑（示例：读取 Kafka 数据）
           // Properties kafkaProps = new Properties();
           // kafkaProps.setProperty("bootstrap.servers", "kafka-node1:9092");
           // kafkaProps.setProperty("group.id", "flink-cluster-group");
           // env.addSource(new FlinkKafkaConsumer<>("test_topic", new SimpleStringSchema(), kafkaProps))
           //    .print();
           
           // 提交任务（集群环境下，execute() 会将 JobGraph 提交给集群 JobManager）
           env.execute("Cluster Flink Job Demo");
       }
   }
   ```

   **提交到集群的命令（示例：Standalone 集群）**

   ```bash
   # 打包后提交（jar 包需要包含所有依赖，或使用 Flink 提供的依赖）
   ./bin/flink run -c com.example.ClusterEnvDemo /path/to/your/flink-job.jar
   ```

   **关键特性**

   - 自动适配部署环境，代码可移植性强
   - 充分利用集群资源，支持大规模数据处理和高可用
   - 需提前搭建和配置 Flink 集群，依赖集群环境

3. **远程执行环境（Remote Execution Environment）**

   手动指定远程集群的 JobManager 地址，直接将任务提交到远程集群运行，**较少用于生产环境**（灵活性低，不如 getExecutionEnvironment() 适配性好），偶尔用于本地直接提交任务到测试集群

   **核心创建方式**

   ```JAVA
   
   public class RemoteEnvDemo {
   
       public static void main(String[] args) throws Exception {
           // 创建远程执行环境，指定 JobManager 的地址、端口、并行度、依赖 Jar 包
           StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(
                   "192.168.1.1", // JobManager 主机名/IP
                   8081,               // JobManager RPC 端口（默认 8081）
                   2,                  // 全局并行度
                   "xxx.jar"  // 任务依赖的 jar 包（可选）
           );
   
           // 后端业务逻辑
           env.readTextFile("hdfs://192.168.1.1:9000/data/input.txt")
                   .print();
           
           // 提交任务（直接提交到指定的远程 JobManager）
           env.execute("Remote Flink Job Demo");
       }
       
   }
   ```

   **关键特性**

   - 手动指定集群地址，灵活性低，代码与集群耦合
   - 本地无需打包，可直接提交远程集群，适合快速测试集群环境
   - 需保证本地与远程的网络互通，依赖 jar 包需一致

#### 5.1.3 执行环境的核心常用 API

不管哪种执行环境，都提供了一系列的通用 API，用于配置任务和创建数据源，这里列举最核心的几个：

1. **配置相关 API**

   - **setParallelism(int parallelism)**：设置全局并行度（可被算子级并行度、集群配置覆盖）
   - **setMaxParallelism(int maxParallelism)**：设置最大并行度（用于任务扩容时的并行度调整，避免状态迁移问题）
   - **enableCheckpointing(long interval)**：开启检查点（流处理容错核心），指定检查点间隔（毫秒）
   - **setStateBackend(StateBackend stateBackend)**：设置状态后端（用于存储任务状态，如 RocksDBStateBackend）
   - **getConfig**：获取执行环境配置对象，可配置更多细节（如超时时间、序列化方式）

2. **数据源创建 API（核心）**

   - **`readTextFile(String path)`**：读取文本文件（本地文件、HDFS 等）。

   - **`fromCollection(Collection<T> collection)`**：从 Java 集合中读取数据（仅用于测试，不支持大规模数据）。

   - **`fromElements(T... elements)`**：直接从指定元素中创建数据源（仅用于测试）。

   - **`addSource(SourceFunction<T> source)`**：添加自定义数据源（或内置数据源，如 Kafka、Redis 等），流处理的核心数据源方式

3. **任务提交 API**

   - **`execute(String jobName)`**：提交任务并阻塞等待执行完成，返回任务执行结果（`JobExecutionResult`），包含任务耗时、并行度等信息。

   - **`executeAsync(String jobName)`**：异步提交任务，不阻塞当前线程，返回 `JobClient`，可用于后续查询任务状态（Flink 1.14+ 支持）

#### 5.1.4 执行模式

**统一流批处理的执行模式切换**：Flink 1.12+ 中，`StreamExecutionEnvironment` 支持通过 `setRuntimeMode(RuntimeExecutionMode mode)` 切换执行模式

- `STREAMING`：流处理模式（默认，处理无界流，实时计算）。
- `BATCH`：批处理模式（处理有界流，类似传统 MapReduce，结果一次性输出）。

- `AUTOMATIC`：自动判断（根据数据源是否有界，自动切换流 / 批模式）



设置执行模式有两种方式：

1. 代码设置 `setRuntimeMode(RuntimeExecutionMode mode)`

2. 在提交作业的时候，通过参数指定

   ```bash
   -Dexecution.runtime-mode=BATCH
   ```

#### 5.1.5 关键注意点（避坑指南）

1. **一个应用只能有一个执行环境**：Flink 应用中，`StreamExecutionEnvironment` 或 `ExecutionEnvironment` 只能创建一个，多个执行环境会导致冲突和异常。

2. **`execute()` 是任务执行的触发点**：Flink 应用的逻辑是「惰性执行」的，只有调用 `execute()` 方法，才会将之前定义的算子、数据源等转换成 JobGraph 并提交运行，不调用 `execute()` 则任务不会执行。

3. **统一流批处理的执行模式切换**：Flink 1.12+ 中，`StreamExecutionEnvironment` 支持通过 `setRuntimeMode(RuntimeExecutionMode mode)` 切换执行模式：

   - `STREAMING`：流处理模式（默认，处理无界流，实时计算）。
   - `BATCH`：批处理模式（处理有界流，类似传统 MapReduce，结果一次性输出）。

   - `AUTOMATIC`：自动判断（根据数据源是否有界，自动切换流 / 批模式）

4. **本地环境与集群环境的资源隔离**：本地环境运行时，不要处理大规模数据，避免占用过多本地资源导致 OOM；集群环境运行时，需注意配置资源（内存、CPU）与集群节点的匹配



### 5.2 源算子（Source）

​	Flink 可以从各种来源获取数据，然后构建 DataStream 进行转换处理，一般将数据的输入来源称之为 （Data Source），而读取数据的算子就是**源算子（Source Operator）**。所以，Source就是我们整个处理程序的输入端

​	在 Flink 1.12 以前，旧的添加 Source 的方式是调用执行环境的 addSource() 方法：

`DataStream<String> stream = env.addSource(...)`;

​	方法传入的参数是一个 “源函数”（Source Function），需要实现 SourceFunction 接口

​	从 Flink 1.12 开始，主要使用流批一体的新 Source 架构：

`DataStreamSource<String> stream = env.fromSource(...)`

​	Flink 直接提供了很多预实现的接口，才外还有很多外部连接工具也帮我们实现了对应的 Source，通常情况下足以应对我们的实际需求

#### 5.2.1 核心概念

1. **Source算子的作用**：作为 Flink 作业的 “数据输入端”，负责连接外部数据源、读取/采集数据，并将数据封装为 Flink 内部的 `DataStream`(流处理) 或 `Table`/`DataStream`(批处理，Flink 批流一体)，供后续转换（Transform）、输出（Sink）算子处理
2. **批流一体特性**：Flink 1.23+ 实现了批流一体，`Source`也支持批处理场景（如读取本地文件、HDFS静态文件）和流处理场景（如读取Kafka、CDC实时数据），底层通过统一的 `SourceFunction` 或 `Source` 接口实现
3. **核心接口**：
   - **低阶接口（早期实现，多用于简单场景）**：`SourceFunction`（无并行度，单线程）、`ParallelSourceFunction`（支持并行度）、`RichParallelSourceFunction`（扩展了生命周期方法，如 `open`/`close`，支持资源初始化/释放）
   - **高阶接口（Flink 1.14+推荐，批流一体、支持更丰富特性）**：`Source` （统一批流接口）、`StreamSource`（流场景封装），配套 `SourceBuilder` 简化构建

#### 5.2.2 源算子分类

按照数据源的类型和使用场景，可分为 3 大类，覆盖绝大数实际开发需求

##### 5.2.2.1 内置基础数据源（用于简单测试/本地调试）

主要用于测试、简单场景，支持批/流模式，核心包括一下集中

1. **从集合读取（fromCollection）**

   读取内存的 Java/Scala 集合（如 List、Set），**多用于本地测试**，不适合生产环境（数据量有限、无持久化）

   ```java
   import org.apache.flink.streaming.api.datastream.DataStreamSource;
   import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
   import java.util.Arrays;
   import java.util.List;
   
   public class CollectionSourceDemo {
       
       public static void main(String[] args) throws Exception {
           // 1. 获取执行环境
           StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
           // 设置并行度（默认并行度为 CPU 核心数）
           env.setParallelism(1);
           
           // 2. 从 List 集合创建 Source
           List<String> dataList = Arrays.asList("Flinks", "Source", "Collection", "Demo");
           DataStreamSource<String> ds = env.fromCollection(dataList);
           
           // 3. 打印输出（Sink）
           ds.print("Collection Source:");
           
           // 4. 执行作业（Flink 作业必须显式执行）
           env.execute("CollectionSourceDemo");
       }
       
   }
   
   ```

   输出结果（按集合顺序输出）：

   ```tex
   Collection Source:> Flink
   Collection Source:> Source
   Collection Source:> Collection
   Collection Source:> Demo
   ```

   

2. **从元素直接创建（fromElements）**

   无需封装集合，直接传入多个离散元素，**测试场景比 fromCollection 更简洁**

   ```java
   // 替代 fromCollection，直接传入元素
   DataStream<String> dataStream = env.fromElements("Flink", "Source", "Elements", "Demo");
   ```

3. **从文件读取（readTextFile/readFile）**

   读取本地文件或分布式文件系统（HDFS、S3等），支持批模式（读取静态文件，一次性读取完毕）和流模式（监控文件目录，读取新增文件/新增内容）

   - **批模式（默认）**：读取指定文件的全部内容，适合处理静态数据

     ```java
     // 读取本地文件（也可传入 hdfs://xxx/xxx.txt，读取 HDFS 文件）
     DataStream fileStream = env.readTextFile("src/main/resources/test.txt");
     ```

   - **流模式：通过 readFile 配置监控策略，适合处理增量文件数据**

     ```java
     import org.apache.flink.core.fs.Path;
     import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
     import java.util.concurrent.TimeUnit;
     
     // 流模式：监控指定目录，每 10 扫描一次新增文件/新增内容
     DataStream<String> fileStream = env.readFile(
     	org.apache.flink.api.common.io.TextInputFormat.class, // 文件输入格式（文本文件默认）
         "src/main/resources/file-dir",	// 监控目录
         FileProcessingMode.PROCESS_CONTINUOUSLY, // 流处理模式（持续监控）
         TimeUnit.SECONDS.toMillis(10)	// 扫描间隔
     )
     ```

   - **批流一体（File Source）**

     ```java
     import org.apache.flink.api.common.eventtime.WatermarkStrategy;
     import org.apache.flink.connector.file.src.FileSource;
     import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
     import org.apache.flink.core.fs.Path;
     import org.apache.flink.streaming.api.datastream.DataStreamSource;
     import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
     
     public class FileSourceDemo {
         public static void main(String[] args) throws Exception {
             // 1. 获取执行环境
             StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
             env.setParallelism(1);
             
             // 2. 构建 File Source（读取文本文件，支持本地文件/HDFS）
             FileSource<String> fileSource = FileSource.forRecordStreamFormat(
                     new TextLineInputFormat(),  // 输入格式：按行读取文本
                     new Path("data/input/demo.txt") // 文件路径
             ).build();
             
             // 3. 构建数据流
             DataStreamSource<String> fileStream = env.fromSource(
                     fileSource,
                     WatermarkStrategy.noWatermarks(),
                     "File Source Demo"
             );
             
             // 4. 打印输出
             fileStream.print("FileStream:");
             
             // 5. 执行作业
             env.execute("FileSourceDemo");
         }
     }
     ```

4. **从 Socket 读取（socketTextStream）**

   读取 Socket 端口发送的数据，**多用于本地实时测试**（生产环境不推荐，无容错、无持久化）

   ```bash
   # 先在终端启动 Socket 服务（Linux/Mac）
   nc -lk 9999
   ```

   ```java
   // 连接本地 9999 端口，读取 Socket 数据
   DataStream<String> socketStream = env.socketTextStream("localhost", 9999);
   ```

   此时在终端输入内容，Flink 作业会实时接收并打印

##### 5.2.2.2 第三方连接器数据源（生产环境主流）

针对生产环境中的常用数据（如 Kafka、MySQL CDC、HBase等），Flink 提供了专用的连接器（Connector），需要引入额外的 Maven 依赖，支持高可用、容错、并行读取等生产级特性

1. **Kafka Source（最常用，流处理核心）**

   Flink 提供了两个版本的 Kafka 连接器

   - 旧版：基于 `FlinkKafkaConsumer`（实现 `SourceFunction`），兼容低版本 Flink
   - 新版：Flink 1.14+ 推出的 `KafkaSource`（基于统一 `Source` 接口，批流一体，推荐生产环境使用 ）

   下面是新版 `KafkaSource` 的完整实例（依赖 Flink 1.17+）

   **第一步：引入 Maven 依赖**

   ```xml
   <!-- Flink Kafka 连接器（核心） -->
   <dependency>
       <groupId>org.apache.flink</groupId>
       <artifactId>flink-connector-kafka</artifactId>
       <version>1.17.0</version>
   </dependency>
   <!-- Kafka 客户端（需与 Kafka 集群版本兼容） -->
   <dependency>
       <groupId>org.apache.kafka</groupId>
       <artifactId>kafka-clients</artifactId>
       <version>3.2.0</version>
   </dependency>
   ```

   **第二部：编写代码**

   ```java
   import org.apache.flink.api.common.eventtime.WatermarkStrategy;
   import org.apache.flink.api.common.serialization.SimpleStringSchema;
   import org.apache.flink.connector.kafka.source.KafkaSource;
   import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
   import org.apache.flink.streaming.api.datastream.DataStreamSource;
   import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
   
   public class KafkaSourceDemo {
       public static void main(String[] args) throws Exception {
           // 1. 获取执行环境
           StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
           // KafkaSource 支持并行读取数据（并行度 <= Kafka 分区数）
           env.setParallelism(2);
           
           // 2. 构建 KafkaSource
           KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                   .setBootstrapServers("192.168.1.1:9092") // Kafka 集群地址
                   .setTopics("test")  // 要消费的主体
                   .setGroupId("test") // 消费者组 ID
                   // 设置初始偏移量: earliest(从最早开始)、latest(从最新开始)、specific(指定偏移量)、committed(已提交的)
                   .setStartingOffsets(OffsetsInitializer.earliest())
                   // 设置反序列器（将 Kafka 消息转化为 Flink 可处理的 String 类型）
                   .setValueOnlyDeserializer(new SimpleStringSchema())
                   .build();
           
           // 3. 从 Kafka Source 创建 DataStream
           DataStreamSource<String> kafkaStream = env.fromSource(
                   kafkaSource, 
                   WatermarkStrategy.noWatermarks(), // 水印策略（简单场景无需水印）
                   "Kafka Source");    // Source 名称（便于监控）
           
           // 4. 打印输出
           kafkaStream.print("Kafka Source:");
           
           // 5. 执行作业
           env.execute("KafkaSourceDemo");
       }
   }
   
   ```

   

2. **CDC Source（读取数据库变更数据，如MySQL、PostgreSQL）**

   **CDC（Change Data Capture）**用于捕获数据库的增删改查（INSERT/UPDATE/DELETE）变更，是实时数据同步，数仓建设的核心场景，Flink 提供了 `flink-connector-cdc`连接器，基于 Debezium 实现

   示例（MySQL CDC Source，Flink 1.17+）

   **Maven依赖**

   ```xml
   <dependency>
       <groupId>com.ververica</groupId>
       <artifactId>flink-connector-mysql-cdc</artifactId>
       <version>2.4.0</version>
   </dependency>
   ```

   ```java
   
   import com.ververica.cdc.connectors.mysql.source.MySqlSource;
   import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
   import org.apache.flink.api.common.eventtime.WatermarkStrategy;
   import org.apache.flink.streaming.api.datastream.DataStreamSource;
   import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
   
   public class MysqlCdcSourceDemo {
   
       public static void main(String[] args) throws Exception {
           // 1. 获取执行环境
           StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
           env.setParallelism(1); // MySQL CDC Source 目前不支持并行读取（单并行度）
   
           // 2. 构建 MySQL CDC Source
           MySqlSource<String> mySqlCdcSource = MySqlSource.<String>builder()
                   .hostname("192.168.1.1")    // MySQL 地址
                   .port(3306)                 // MySQL 端口
                   .username("root")           // MySQL 用户名
                   .password("<PASSWORD>")     // MySQL 密码
                   .databaseList("test")       // 要监控的数据库
                   .tableList("test.user")     // 要监控的表（格式：数据库.表）
                   .deserializer(new JsonDebeziumDeserializationSchema())  // 反序列为 JSON 字符串
                   .build();
           
           // 3. 从 CDC Source 创建 DataStream
           DataStreamSource<String> cdcStream = env.fromSource(mySqlCdcSource, WatermarkStrategy.noWatermarks(), "MySQL CDC Source");
           
           // 4. 打印输出
           cdcStream.print();
           
           // 5. 执行作业
           env.execute("MysqlCdcSourceDemo");
       }
   ```

   当 `tast_db.test` 表发生数据变更时，Flink 会实时捕获并输出变更信息（包括操作类型、旧数据、新数据）

##### 5.2.2.3  <span style="color:red">**自定义 Source （满足特殊场景需求）**</span>

当内置数据源和第三方连机器无法满足需求时（如读取自定义协议的设备数据、私有系统接口数据），可以通过实现 Flink 提供的 Source 接口来定义 Source。

**推荐两种实现方式**：

1. **低阶实现：RichParallelSourceFunction（简单场景，易于理解）**

   实现 `run()`方法（数据生成/读取逻辑）和 `cancel()` 方法（取消作业时的资源释放逻辑），`Rich` 前缀提供了 `open()`/`close()` 生命周期方法

   示例：自定义一个生成递增数字的 Source （每秒生成一个数字）

   ```java
   import org.apache.flink.streaming.api.datastream.DataStreamSource;
   import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
   import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
   
   public class CustomSourceDemo {
       public static void main(String[] args) throws Exception {
           // 1. 获取执行环境
           StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
           env.setParallelism(1);
           
           // 2. 自定义数据源
           DataStreamSource<Long> myCustomSource = env.addSource(new MyCustomSource(), "MyCustomSource");
           
           // 3. 打印输出
           myCustomSource.print("Custom Souorce:");
           
           // 4. 执行作业
           env.execute("CustomSourceDemo");
       }
       
       // 自定义 Source: 实现 RichParallelSourceFunction
       public static class MyCustomSource extends RichParallelSourceFunction<Long> {
           // 标记是否继续运行（用于取消作业时终止循环）
           private volatile boolean isRunning = true;
           // 递增数字
           private long count = 0;
           
           @Override
           public void run(SourceContext<Long> ctx) throws Exception {
               while (isRunning) {
                   // 加锁写入数据（保证并发场景下的数据一致性）
                   synchronized (ctx.getCheckpointLock()) {
                       ctx.collect(count); // 发送数据到下游算子
                       count++;
                   }
                   // 每秒生产一个数据
                   Thread.sleep(1000);
               }
           }
   
           @Override
           public void cancel() {
               // 取消作业时，将标记设置为 false，终止 run() 方法中的循环
               isRunning = false;
           }
       }
   }
   
   ```

   输出结果：每秒输出一个递增的数字，停止作业时会触发 `cancel()` 方法释放资源

2. <span style="color:red">**高阶实现：Source（Flink 1.14+ 推荐，批流一体）**</span>

   基于 `SourceBuilder` 构建，支持更丰富的的特性（如 分区发现、水印生成、容错优化），适合复杂生产场景，步骤相对繁琐，这里不展开细节，核心思路是实现 `SplitEnumerator`（分区枚举，分配任务）和 `Reader` （数据读取，处理单个分区数据）

#### 5.2.3 从数据生成器读取数据

Flink 从 1.11 开始提供了一个内置的 DataGen 连接器，主要是用于生成一些随机数，用于在没有数据源的时候，进行流任务的测试以及性能测试等。1.17 提供了新的 `Source` 写法，需要导入依赖

```xml
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataGenSimpleDemo {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataGeneratorSource<String> generatorSource = new DataGeneratorSource<>(new GeneratorFunction<Long, String>() {
            @Override
            public String map(Long value) throws Exception {
                return "数据 -> " + value;
            }
        }, 
                100,
                RateLimiterStrategy.perSecond(10),
                TypeInformation.of(String.class));

        DataStreamSource<String> dataStreamSource = env.fromSource(
                generatorSource,
                WatermarkStrategy.noWatermarks(),
                "DataGen Source"
        );

        dataStreamSource.print();
        env.execute();
    }
}
```

#### 5.2.4 Flink 支持的数据类型

1. **Flink 的类型系统**

   Flink 使用 “类型信息” （TypeInformation）来统一表示数据类型。TypeInfomation 类是 Flink 中所有类型描述符的基类。它涵盖了类型的一些基本属性，并为每个数据类型生成特定的序列化器、反序列化器和比较器

2. **Flink 支持的数据类型**

   对于常见的 Java 和 Scala 数据类型，Flink 都是支持的。Flink 在内部，Flink 对支持不同的类型进行了划分，这些类型可以在 Types 工具类中找到

   - 基本类型

     所有 Java 基本类型及其包装类，再加上 Void、String、Date、BigDecimal 和 BigInteger

   - 数组类型

     包括基本数据数据（PRIMITIVE_ARRAY） 和对象数组（OBJECT_ARRAY）

   - 复合数据类型

     - Java 元组类型（TUPER）：这是 Flink 内置的元组类型，是 Java API 的一部分，最多25个字段，也就是从 Tuple0 ~ Tuple25，不支持空字段
     - Scala 样例类及 Scala 组：不支持空字段
     - 行类型（ROW）：可以认为是具有任意字段的元组，并支持空字段
     - POJO：Flink 自定义的类似于 Java Bean 模式的类

   - 辅助类型

     Option、Either、List、Map等

   - 泛型类型（GENERIC）

     ​	Flink 支持所有 Java 类和 Scala 类。不过如果没有按照上面 POJO 类型的要求来定义，就会被 Flink 当做泛型类来处理。Flink 会把泛型类型当做黑盒，无法获取它们内部的属性；它们也不是由 Flink 本身序列化的，**而是由 Jryo 序列化的**

     ​	在这些类型中，元组类型和 POJO 类型最为灵活，因为它们支持创建复杂类型。而相比之下，POJO 还支持在键（key）的定义中直接使用字段名，这会让我们的代码可读性大大增加。所以，在项目实践中，往往会将流处理程序的元素类型定位 Flink 的 POJO 类型

     **Flink 对 POJO 类型的要求如下：**

     - 类是公有（public）的
     - 有一个无参的构造函数
     - 所有属性都是公有（public）的
     - 所有属性都是可以序列化的

3. 类型提示（Type Hints）

   ​	Flink 还具有一个类型提取系统，可以分析函数的输入和返回类型，自动获取类型信息，从而获得对应的序列化器和反序列化器。但是，由于 Java 中泛型的擦除的存在，在某些特殊情况下（比如 Lambda 表达式），自动提取的信息是不够精细的 -- 只告诉 Flink 当前的元素由 “船头、船身、船尾”构成，根本无法重建出 “大船” 的模样；这时就需要显式地提供类型信息，才能使应用程序正常工作或提高其性能

   ​	为了解决这类问题，Java API 提供了专门的 “类型提示”（type hints）

### 5.3 转换算子（Transformation）

#### 5.3.1 基本转换算子（map/filter/flatMap）

##### 5.3.1.1 映射（map）

- **功能**：对数据流重大**每一个元素**进行一对一的转换，输入一个元素，输出一个元素

- **数据场景**：数据格式转换、字段提取、简单值修改（如类型转换、数值计算）

- **代码示例**：

  ```java
  import org.apache.flink.streaming.api.datastream.DataStream;
  import org.apache.flink.streaming.api.datastream.DataStreamSource;
  import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
  
  public class MapOperatorDemo {
      public static void main(String[] args) throws Exception {
          // 1. 获取执行环境
          StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
          env.setParallelism(1);
          
          // 2. 构建输入数据流
          DataStreamSource<Integer> inputStream = env.fromElements(1, 2, 3, 4, 5);
          
          // 3. 适用 Map 算子：将每个整数乘以2（一对一转换）
          DataStream<Integer> map = inputStream.map(x -> x * 2);
          
          // 4. 打印输出
          map.print("map算子输出");
          
          // 5. 执行任务
          env.execute();
      }
  }
  ```

- 输出结果

  ```te
  map算子输出> 2
  map算子输出> 4
  map算子输出> 6
  map算子输出> 8
  map算子输出> 10
  ```

##### 5.3.1.2 过滤（filter）

- **功能**：对数据流中的每个元素进行条件判断，**保留满足条件的元素**，过滤掉不满足条件的元素

- **适用场景**：数据清洗（如过滤空值、过滤不符合业务规则的数据）

- **代码示例**：

  ```java
  import org.apache.flink.streaming.api.datastream.DataStream;
  import org.apache.flink.streaming.api.datastream.DataStreamSource;
  import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
  
  public class FilterOperatorDemo {
      public static void main(String[] args) throws Exception {
          StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
          env.setParallelism(1);
  
          DataStreamSource<Integer> inputStream = env.fromElements(1, 2, 3, 4, 5, 6);
          
          // filter 算子：保留偶数（满足 num % 2 == 0 的元素）
          DataStream<Integer> filter = inputStream.filter(num -> num % 2 == 0);
          
          filter.print("filter算子输出");
          env.execute();
      }
  }
  ```

- 输出结果

  ```te
  filter算子输出> 2
  filter算子输出> 4
  filter算子输出> 6
  filter算子输出> 8
  ```

##### 5.3.1.3 扁平映射（flatMap）

-  **功能**：对数据流中的**每一个元素**进行一对多的转换，输出一个元素，输出 0 个、1个或多个元素（以集合/迭代器形式返回）

- **适用场景**：数据拆分（如一行文本拆分成多个单词）、数据过滤（输出0个元素即过滤掉该输入）

- **代码示例（文本拆分）**：

  ```java
  import org.apache.flink.api.common.functions.FlatMapFunction;
  import org.apache.flink.streaming.api.datastream.DataStream;
  import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
  import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
  import org.apache.flink.util.Collector;
  
  public class FlatMapOperatorDemo {
      public static void main(String[] args) throws Exception {
          StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
          env.setParallelism(1);
          
          // 输入数据流：两行文本
          DataStream<String> inputStream = env.fromElements("hello world", "hello flink");
          
          // flatMap算子：将每行文本按空格拆分，输出单个单词
          SingleOutputStreamOperator<String> resultStream = inputStream.flatMap(new FlatMapFunction<String, String>() {
              @Override
              public void flatMap(String s, Collector<String> collector) throws Exception {
                  // 拆分文本
                  String[] words = s.split(" ");
                  // 遍历拆分结果，逐个输出
                  for (String word : words) {
                      collector.collect(word);
                  }
              }
          });
          
          resultStream.print("flatMap算子输出");
          env.execute("FlatMap Operator Demo");
      }
  }
  ```

- 输出结果

  ```tex
  flatMap算子输出> Hello
  flatMap算子输出> Flink
  flatMap算子输出> Hello
  flatMap算子输出> Big
  flatMap算子输出> Data
  ```

##### 5.3.1.4 flatMap + filter 组合（常用）

实际开发中常将两者组合使用，先拆分在过滤，例如：过滤拆分后的空单词



#### 5.3.2 聚合算子（Aggregation）

这类算子用于对数据流中的元素进行聚合计算，**通常需要结合 【键（key）】适用**（即先分区，再聚合），无键聚合仅适用于特殊场景（如全局聚合，不推荐大规模流处理中使用）

![](E:\Idea\Idea_Study\Flink\image-2026012517255980633.png)

##### 5.3.2.1 按键分组（keyBy）

`keyBy` 不是聚合算子，但它是绝大多数聚合算子的前置操作：

- **功能**：根据指定的**键（key）**将数据划分成多个逻辑分区（KeyedStream），相同键的元素会被分配到同一个分区中，后续的聚合操作仅在各自分区内进行

- **注意**：

  - 支持基于字段名（POJO类）、字段索引（数组/元组）、Lambda 表达式指定键
  - 不能用于不可哈希的数据类型（如数组、自定义对象未重写 `hashCode` 方法）

- **代码示例（基于元组字段索引）**

  ```java
  // 构建元组数据流：（单词，出现次数）
  DataStream<Tuple2<String, Integer>> inputStream = env.fromElements(
  	Tuple2.of("Hello", 1),
      Tuple2.of("Flink", 1),
      Tuple2.of("Hello", 1)
  );
  
  // keyBy: 根据第 0 个字段（单词）分区
  KeyedStream<Tuple2<String, Integer>> keyedStream = inputStream.keyBy(t -> t.f0);
  ```

##### 5.3.2.2 简单聚合（sum/min/max/minBy/maxBy）

**基于 KeyedStream**

1. **sum**

   - **功能**：对指定字段进行累加求和

   - **代码示例**

     ```java
     import org.apache.flink.api.java.tuple.Tuple2;
     import org.apache.flink.streaming.api.datastream.DataStream;
     import org.apache.flink.streaming.api.datastream.KeyedStream;
     import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
     
     public class SumOperatorDemo {
         public static void main(String[] args) throws Exception {
             StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
             env.setParallelism(1);
             // 输入数据流
             DataStream<Tuple2<String, Integer>> inputStream = env.fromElements(
                     Tuple2.of("Hello", 1),
                     Tuple2.of("Flink", 1),
                     Tuple2.of("Hello", 1),
                     Tuple2.of("Flink", 1),
                     Tuple2.of("Java", 1)
             );
             // 1. keyBy 分区
             KeyedStream<Tuple2<String, Integer>, String> keyedStream = inputStream.keyBy(t -> t.f0);
             
             // 2. sum 算子：对第一个字段（次数）求和
             DataStream<Tuple2<String, Integer>> result = keyedStream.sum(1);
             
             result.print("sum算子输出");
             env.execute();
         }
     }
     ```

   - **输出结果**（累加过程可见）：

     ```tex
     sum算子输出> (Hello,1)
     sum算子输出> (Flink,1)
     sum算子输出> (Hello,2)
     sum算子输出> (Flink,3)
     ```

2. **min/max**

   - **功能**

     - `min`：获取指定字段的最小值，**仅更新聚合字段，其他字段保留第一条数据的值**

     - `max`：获取指定字段的最大值，同样仅更新聚合字段，其它字段保留第一条数据的值

3. **minBy/maxBy**

   - **功能**

     - `minBy`：获取指定字段的最小值对应的**整条记录**，所有字段都会更新为符合条件的记录值
     - `maxBy`：获取指定字段的最大值对应的**整条记录**，所有字段都会更新为符合条件的记录值

   - **区别示例（min vs minBy）**

     ```java
     / 输入数据流：商品名称、价格、库存
     DataStream<Tuple3<String, Integer, Integer>> inputStream = env.fromElements(
     	Tuple3.of("手机", 5000, 100),
         Tuple3.of("电脑", 8000, 50),
         Tuple3.of("手机", 4500, 150)
     );
     
     KeyedStream<Tuples3<String, Integer, Integer>> keyedStream = inputStream.keyBy(t -> t.f0);
     
     // min(1): 仅价格字段取最小值，库存保留第一条记录的100
     DataStream<Tuples3<String, Integer, Integer>> minStream = keyedStream.min(1);
     
     // minBy(1)：价格最小值对应的整条记录，库存更新为 150
     DataStream<Tuples3<String, Integer, Integer>> minByStream = keyedStream.minBy(1);
     ```

   - **输出结果对比**：

     - `min`输出：（手机, 4500, 100）
     - `minBy`输出：（手机, 4500, 150）

##### 5.3.2.3 规约聚合（reduce）

- **功能**：更灵活的聚合操作，对 KeyedStream 中的元素进行**迭代式聚合**，输入两个元素，输出一个元素，最终得到单个聚合结果

- **适用场景**：自定义聚合逻辑（如 累加、平均值、拼接字符串等）

- **代码示例**（实现 sum 相同的功能，更灵活）：

  ```java
  DataStream<Tuple2<String, Integer>> resultStream = keyedStream.reduce((t1, t2) -> {
      // t1：上一次聚合的结果，t2：当前待聚合的元素
      return Tuple2.of(t1.fo, t1.f1 + t2.f1);
  })
  ```

  

#### 5.3.3 用户自定义函数（UDF）

##### 5.3.3.1 函数类（Function Classes）

这是 Flink 自定义函数的**标准实现方式**，通过实现 Flink 提供的专用函数接口，编写业务逻辑，适用于**中等复杂度、无状态**的场景，可读性和可维护性优于 Lambda 表达式

**核心接口（常用）**

Flink 为不同的算子提供了对应的函数接口，核心接口如下（均位于 `org.apache.flink.api.common.functions` 包下）：

|     接口名称      | 对应算子  |                   功能描述                    |
| :---------------: | :-------: | :-------------------------------------------: |
|   `MapFunction`   |   `map`   |    一对一转换，输入一个元素，输出一个元素     |
| `FlatMapFunction` | `flatMap` | 一对多转换，输入一个元素，输出 0/1 / 多个元素 |
| `FilterFunction`  | `filter`  | 条件过滤，返回 `true` 保留元素，`false` 过滤  |
| `ReduceFunction`  | `reduce`  |    迭代式聚合，输入两个元素，输出一个元素     |

**实现步骤**

1. 导入对应的函数接口。
2. 自定义类实现该接口，重写接口中的核心方法（如 `map()`、`flatMap()`）。
3. 将自定义函数类的实例传入对应的 Flink 算子中。

**代码示例（以 `FlatMapFunction` 为例，文本拆分 + 过滤空单词）**

```
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

// 自定义 FlatMap 函数类：实现 FlatMapFunction 接口
class MyFlatMapFunction implements FlatMapFunction<String, String> {
    /**
     * 核心方法：重写 flatMap()
     * @param value 输入元素（每行文本）
     * @param out 输出收集器：用于收集转换后的元素
     */
    @Override
    public void flatMap(String value, Collector<String> out) throws Exception {
        // 1. 非空判断（避免空指针异常）
        if (value == null || value.trim().isEmpty()) {
            return;
        }
        // 2. 按空格拆分文本
        String[] words = value.trim().split(" ");
        // 3. 遍历拆分结果，过滤空单词并输出
        for (String word : words) {
            if (!word.isEmpty()) {
                out.collect(word);
            }
        }
    }
}

public class UdfInterfaceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 输入数据流（包含空文本和空单词）
        DataStream<String> inputStream = env.fromElements(
            "Hello Flink",
            "  Hello Big Data  ",
            "",
            "Flink  UDF   Demo"
        );

        // 传入自定义 FlatMap 函数实例
        DataStream<String> resultStream = inputStream.flatMap(new MyFlatMapFunction());

        resultStream.print("接口实现类 UDF 输出");
        env.execute("UDF Interface Demo");
    }
}
```

**输出结果**

```tex
接口实现类 UDF 输出> Hello
接口实现类 UDF 输出> Flink
接口实现类 UDF 输出> Hello
接口实现类 UDF 输出> Big
接口实现类 UDF 输出> Data
接口实现类 UDF 输出> Flink
接口实现类 UDF 输出> UDF
接口实现类 UDF 输出> Demo
```

**优势**

1. 结构清晰，可读性强，便于复杂业务逻辑的扩展和维护。
2. 支持异常处理、非空判断等复杂逻辑，鲁棒性更高。
3. 类型明确，避免 Lambda 表达式的类型推断问题。



##### 5.3.3.2 富函数类（Rich Function Classes）

这是 Flink 自定义函数的**高级实现方式**，所有的富函数都继承自 `RichFunction` 接口，它在标准接口的基础上，提供了**运行时上下文（Runtime Context）**和**生命周期方法**，适用于**有状态、需要访问运行时信息、需要初始化 / 清理资源**的复杂场景

**核心特性**

1. **生命周期方法**：Flink 会在任务执行的不同阶段自动调用，用于资源的初始化和清理

   - `open(Configuration parameters)`：**任务启动时调用一次**（每个并行任务实例仅调用一次），用于初始化资源（如创建数据库连接、加载配置文件、初始化缓存）
   - `close`：**任务结束时调用一次**（每个并行任务实例仅调用一次），用于清理资源（如关闭数据库连接、释放缓存）
   - `invoke(...)`：**处理每个元素时调用**（对应标准接口的核心方法，如`map()`、`flatMap()`）

2. **运行上下文**：通过 `getRuntimeContext()` 方法获取，可访问：

   - 任务的并行度、任务ID、作业名称
   - 状态管理（Keyed State、Operator State）
   - 广播变量（Broadcast Variables）

   **常用富函数**

   |      富函数接口       |   对应标准接口    | 对应算子  |
   | :-------------------: | :---------------: | :-------: |
   |   `RichMapFunction`   |   `MapFunction`   |   `map`   |
   | `RichFlatMapFunction` | `FlatMapFunction` | `flatMap` |
   | `RichFilterFunction`  | `FilterFunction`  | `filter`  |
   | `RichReduceFunction`  | `ReduceFunction`  | `reduce`  |

   **代码示例（以 `RichMapFunction` 为例，初始化资源 + 访问运行时上下文）**

   ```java
   import org.apache.flink.api.common.functions.RichMapFunction;
   import org.apache.flink.configuration.Configuration;
   import org.apache.flink.streaming.api.datastream.DataStream;
   import org.apache.flink.streaming.api.datastream.DataStreamSource;
   import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
   
   // 自定义富函数类：实现 RichMapFunction 接口（输入 Integer，输出 String）
   class MyRichMapFunction extends RichMapFunction<Integer, String> {
       // 模拟：需要初始化的资源（如数据库连接、配置对象）
       private String taskName;
       private int parallelism;
       private int taskId;
   
       /**
        * 生命周期方法1: open() - 任务启动时调用（每个并行示例仅调用一次）
        * @param parameters 配置参数
        * @throws Exception
        */
       @Override
       public void open(Configuration parameters) throws Exception {
           // 调用父类方法
           super.open(parameters);
   
           // 1. 获取运行时上下文信息
           this.taskName = getRuntimeContext().getTaskName();
           this.parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
           this.taskId = getRuntimeContext().getIndexOfThisSubtask();
           
           // 2. 模拟初始化资源（如加载配置、创建数据库连接）
           System.out.println("===== 任务 " + taskId + " 初始化完成 ======");
       }
   
       /**
        * 核心方法：map() - 处理每个元素时调用
        * @param value 输入元素
        * @return 输出元素
        * @throws Exception
        */
       @Override
       public String map(Integer value) throws Exception {
           // 转换逻辑：拼接数值和运行时信息
           return String.format(
                   "作业：%s | 并行度：%d | 任务ID：%d | 输入只：%d | 转换后值：%d",
                   taskName, parallelism, taskId, value, value * 3
           );
       }
   
       /**
        * 生命周期方法2: close() - 任务停止时调用（每个并行示例仅调用一次）
        * @throws Exception
        */
       @Override
       public void close() throws Exception {
           // 模拟清理资源（如关闭数据库连接、释放内存）
           System.out.println("===== 任务 " + taskId + " 资源清理完成 ======");
           super.close();
       }
   }
   
   public class UdfRichFunctionDemo {
       public static void main(String[] args) throws Exception {
           StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
           // 设置并行度为 2， 便于观察不同任务实例的运行时信息
           env.setParallelism(2);
   
           // 输入数据流
           DataStreamSource<Integer> inputStream = env.fromElements(1, 2, 3, 4, 5);
   
           // 传入自定义的富函数实例
           DataStream<String> stringDataStream = inputStream.map(new MyRichMapFunction());
   
           stringDataStream.print("富函数 UDF 输出");
           env.execute();
       }
   }
   ```

   **输出结果（关键片段）**

   ```tex
   ===== 任务 0 初始化完成 =====
   ===== 任务 1 初始化完成 =====
   富函数 UDF 输出> 作业：Rich Function UDF Demo | 并行度：2 | 任务ID：0 | 输入值：1 | 转换后值：3
   富函数 UDF 输出> 作业：Rich Function UDF Demo | 并行度：2 | 任务ID：1 | 输入值：2 | 转换后值：6
   富函数 UDF 输出> 作业：Rich Function UDF Demo | 并行度：2 | 任务ID：0 | 输入值：3 | 转换后值：9
   ...
   ===== 任务 0 资源清理完成 =====
   ===== 任务 1 资源清理完成 =====
   ```

   **核心适用场景**

   1. **需要初始化 / 清理资源**：如创建 / 关闭数据库连接、Redis 连接、加载本地配置文件。
   2. **需要访问运行时信息**：如获取作业名称、并行度、任务 ID，用于日志记录或监控。
   3. **需要使用状态管理**：如实现有状态的计算（累计求和、去重），这是富函数最核心的优势（后续状态管理会详细展开）。
   4. **需要使用广播变量**：将公共数据（如字典表）广播到所有并行任务，避免重复加载。

3. **自定义函数的通用注意事项**

   1. **序列化要求**：Flink 会将自定义函数序列化后分发到各个 TaskManager 上执行，因此：
      - 自定义函数类必须实现 `Serializable` 接口（Flink 的核心函数接口已默认实现，自定义类无需显式实现）。
      - 自定义函数中的成员变量如果是不可序列化的对象（如 `Connection`），应在 `open()` 方法中初始化，而非在构造方法中。
   2. **无状态设计优先**：除非业务需要，否则尽量实现无状态的自定义函数，避免状态管理带来的复杂度，同时提高作业的容错性和可扩展性。
   3. **异常处理**：在核心业务逻辑中添加适当的异常处理（如 `try-catch`），避免单个元素的处理异常导致整个任务失败。
   4. **性能优化**：
      - 避免在 `map()`、`flatMap()` 等高频调用方法中创建对象（如 `new String()`），应在 `open()` 方法中初始化可复用对象。
      - 对于大规模数据处理，尽量减少不必要的数据拷贝和复杂计算。

#### 5.3.4 物理分区算子（Physical Paritioning）

##### 5.3.4.1 随机分区（shuffle）

**核心作用**

将数据**随机、均匀地**分发到下游算子的各个并行任务中，打破数据的原有顺序和分区规律

**关键特性**

- 分发结果无规律，每次运行结果可能不同
- 能保证数据大体均匀分布，常用于解决简单的数据倾斜（但不解决基于键的倾斜，仅做均匀打散）
- 会破坏数据的局部性，带来一定的网络传输开销

**使用场景**

- 上游数据分区不均匀，需要简单打散以避免下游部分任务负载过高
- 不关心数据后续处理的任务归属，仅需要均匀分发

**代码示例**

```java
public class ShufflePartitionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置下游算子的并行度为 3（方便看到分区效果）
        env.setParallelism(3);
        
        // 生成测试数据
        DataStreamSource<Integer> inputStream = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        
        // 执行随机分区
        DataStream<Integer> shuffleStream = inputStream.shuffle();
        
        // 打印结果（查看数据分发到哪个并行任务）
        shuffleStream.print("shuffle分区结果");
        
        env.execute("Flink Shuffle Partition Demo");
    }
}
```

**输出特点**

数据会被随机分配到 3 个并行任务中（输出前缀为并行任务编号），例如：

```tex
shuffle分区结果> 2> 1
shuffle分区结果> 1> 2
shuffle分区结果> 0> 3
shuffle分区结果> 2> 4
```

##### 5.3.4.2 轮训分区（rebalance）

**核心作用**

采用**Round-Robin（轮询）**策略，将数据依次、均匀地分发到下游算子的各个并行任务中，是解决数据倾斜的常用算子

**关键特性**

- 分发规则固定（依次循环），数据均匀性比 `Shuffle()` 更有保障
- 属于**重平衡分区**，会重新规划数据分发路径，解决上游算子任务负载不均匀导致的下游数据倾斜
- 有轻微的网络开销，但整体性能优于 `Shuffle`

**使用场景**

- 上游算子各任务输出数据量差异较大（数据倾斜），需要下游均匀接收
- 希望数据按固定顺序均匀分发，不依赖随机策略

**示例代码**

```java
public class RebalancePartitionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置下游算子的并行度为 3（方便看到分区效果）
        env.setParallelism(3);

        // 生成测试数据
        DataStreamSource<Integer> inputStream = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        // 执行轮训分区
        DataStream<Integer> shuffleStream = inputStream.rebalance();

        // 打印结果（查看数据分发到哪个并行任务）
        shuffleStream.print("rebalance分区结果");

        env.execute("Flink Rebalance Partition Demo");
    }
}
```

**输出特点**

```tex
rebalance分区结果> 0> 1
rebalance分区结果> 1> 2
rebalance分区结果> 2> 3
rebalance分区结果> 0> 4
rebalance分区结果> 1> 5
```

##### 5.3.4.3 重缩放分区（rescale）

**核心作用**

采用**局部轮询**策略，仅在相邻的算子任务之间进行轮询分发，是一种轻量级的重分区算子

**关键特性**

- 与 `rebalance()`的区别是：`rebalance` 是全局轮询（上游所有任务对下游所有任务轮询），`rescle()` 是局部轮询（上游一个任务仅对下游部分任务轮询）
- 网络开销远小于 `rebalance` 和 `shuffle`，因为它优先利用数据本地性，减少跨节点传输
- 均匀性略逊于 `rebalance()`，仅适用于上下游算子并行度成**整倍数**的场景（效果最佳）

**使用场景**

- 上下游算子并行度成整倍数（例如上游并行度2，下游并行度 4），需要轻量级均匀分发
- 注重性能，希望减少网络传输开销，且对数据均匀性要求不是极致严格

**示例代码**

```java
public class RescalePartitionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置下游算子的并行度为 3（方便看到分区效果）
        env.setParallelism(3);

        // 生成测试数据
        DataStreamSource<Integer> inputStream = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        // 执行缩放分区
        DataStream<Integer> shuffleStream = inputStream.rebalance();

        // 打印结果（查看数据分发到哪个并行任务）
        shuffleStream.print("Rescale分区结果");

        env.execute("Flink Rescale Partition Demo");
    }
}
```

**补充说明**

假设上游并行度 2，下游并行度 4：

- 上游任务 0 会轮询分发数据到下游任务 0、1
- 上游任务 1 会轮询分发数据到下游任务 2、3
- 避免了跨任务组的网络传输，提高效率

##### 5.3.4.4 广播（broadcast）

**核心作用**

将**上游的每一条数据**都复制一份，分发到下游算子的**所有并行任务**中，下游每个任务都会收到全量的上游数据

**关键特性**

- 数据会被复制多份（复制分数 = 下游并行度），网络开销和内存开销极大，需谨慎使用
- 无需指定下游并行度，下游所有任务都会接收全量数据
- 不会改变数据的原有内容，仅做全量复制分发

**使用场景**

- 分发配置数据、规则数据（例如风控规则、字典表），下游每个任务都需要依赖全量配置进行处理
- 小批量静态数据的分发，不适合大数据量流数据

**示例代码**

```java
public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        // 配置数据（需要广播的全量数据）
        DataStreamSource<String> configStream = env.fromElements("rule1: 大于100", "rule2: 小于0", "rule3: 偶数");

        // 执行广播分区
        DataStream<String> broadcastStream = configStream.broadcast();

        // 业务数据
        DataStreamSource<Integer> businessStream = env.fromElements(101, -5, 20, 300);

        // 下游任务结合广播数据处理（这里仅打印广播数据）
        businessStream.map(value -> {
            // 实际业务中可获取广播的配置数据进行判断
            return "业务数据 " + value + ", 需结合广播规则处理";
        }).print("结合广播数据");

        env.execute("Flink Broadcast Partition Demo");
    }
```

**输出特点**

下游每个并行任务都会收到全量的广播配置数据，无数据丢失

##### 5.3.4.5 全局分区（global）

**核心作用**

将**所有上游数据**都分发到下游算子的**第一个并行任务（编号 0）**中，是一种特殊的分区策略

**关键特性**

- 会导致下游单个任务负载极高，极易出现数据倾斜和任务瓶颈
- 仅适用于特殊场景，一般不推荐在生产环境中使用（除了必须单任务处理的场景）
- 并行度会被强制收敛为 1（下游算子即使设置更高并行度，也仅会有一个任务工作）

**使用场景**

- 下游需要对全量数据进行聚合（例如全局统计总条数、全局排序）
- 调试阶段，需要查看全量数据的完整流转过程

**示例代码**

```java
DataStream<Integer> globalStream = inputStream.global();
```

**输出特点**

所有数据都会被输出到任务 0 中，例如：

```tex
global分区结果> 0> 1
global分区结果> 0> 2
global分区结果> 0> 3
```



##### 5.3.4.6 自定义分区（custom）（键控分区 partitionCustom()）

Flink 提供了 `partitionCustom()` 方法，支持基于自定义规则进行分区，属于物理分区的扩展

**核心作用**

允许用户自定义数据分发规则，满足特殊场景的分区需求，灵活性最高

**实现方式**

需要实现 `Partitioner<T>` 接口（T 为分区键的类型），重写 `int partition(T key, int numPartitions)` 方法

- **`key`**：用户分区的关键字段
- **`numPartitions`**：下游算子的并行任务数量
- **返回值**：下游并行任务的编号（0 - numPartitions），指定数据分发到那个任务

**适用场景**

- 内置分区算子无法满足的特殊分发需求（例如按地区分区、按数据范围分区）
- 需要更精准地控制数据的任务归属，以优化业务处理效率

**示例代码（按数据范围分区）**

需求：将数据 0 ~ 3分发到任务 0，4 ~ 6 分发到任务 1，7 ~ 9 分发到任务 2

```java
public class CustomPartitionNumDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        DataStream<Integer> inputStream = env.fromElements(1, 5, 8, 2, 6, 9, 3, 4, 7);

        // 自定义分区：按数据范围分发
        DataStream<Integer> userDataStream = inputStream.partitionCustom(
                // 第一个参数：自定义 Partitioner 实现类
                new Partitioner<Integer>() {
                    /**
                     *
                     * @param key 用于分区的关键字段
                     * @param numPartitions 下游算子的并行任务数量
                     * @return 下游并行任务的编号（0 ~ numPartitions-1），指定数据分发到哪个任务
                     */
                    @Override
                    public int partition(Integer key, int numPartitions) {
                        // 自定义分区规则
                        int partitions = key % numPartitions;
                        System.out.println("key: " + key + ", numPartitions: " + numPartitions + ", key % numPartitions: " + partitions);
                        return partitions;
                    }
                },
                // 第二个参数：指定分区键（这里使用用户的ID作为分区键）
                new KeySelector<Integer, Integer>() {
                    @Override
                    public Integer getKey(Integer num) throws Exception {
                        return num;
                    }
                }
        );

        userDataStream.print("自定义分区结果");
        env.execute("Flink Custom Partition Demo");
    }

    @Data
    @AllArgsConstructor
    public static class User {
        public int id;
        public String name;
    }

}
```

**输出特点**

数据会严格按照自定义规则分发对应并行任务，例如：

```tex
自定义分区结果> 0> 1
自定义分区结果> 1> 5
自定义分区结果> 2> 8
自定义分区结果> 0> 2
```

**案例二**

```java
public class CustomPartitionDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        DataStream<User> inputStream = env.fromElements(
                new User(1, "张三"),
                new User(2, "李四"),
                new User(3, "王五"),
                new User(4, "赵六"),
                new User(5, "孙七"),
                new User(6, "周八"),
                new User(7, "吴九"),
                new User(8, "郑十"),
                new User(9, "小十"),
                new User(10, "小十一")
        );

        // 自定义分区：按数据范围分发
        DataStream<User> userDataStream = inputStream.partitionCustom(
                // 第一个参数：自定义 Partitioner 实现类
                new Partitioner<Integer>() {
                    /**
                     *
                     * @param key 用于分区的关键字段
                     * @param numPartitions 下游算子的并行任务数量
                     * @return 下游并行任务的编号（0 ~ numPartitions-1），指定数据分发到哪个任务
                     */
                    @Override
                    public int partition(Integer key, int numPartitions) {
                        // 自定义分区规则
                        int partitions = key % numPartitions;
                        System.out.println("key: " + key + ", numPartitions: " + numPartitions + ", key % numPartitions: " + partitions);
                        return partitions;
                    }
                },
                // 第二个参数：指定分区键（这里使用用户的ID作为分区键）
                new KeySelector<User, Integer>() {
                    @Override
                    public Integer getKey(User user) throws Exception {
                        return user.getId();
                    }
                }
        );

        userDataStream.print("自定义分区结果");
        env.execute("Flink Custom Partition Demo");
    }

    @Data
    @AllArgsConstructor
    public static class User {
        public int id;
        public String name;
    }

}
```

##### 5.3.4.7 核心对比

|        算子         |      分发策略      | 网络开销 | 数据均匀性 |             典型场景             |
| :-----------------: | :----------------: | :------: | :--------: | :------------------------------: |
|     `shuffle()`     |      全局随机      |    中    |    较好    |    简单打散数据，避免轻微倾斜    |
|    `rebalance()`    |      全局轮询      |    中    |     好     |    解决严重数据倾斜，均匀分发    |
|     `rescale()`     |      局部轮询      |    低    |    较好    | 上下游并行度成整数倍，轻量级分发 |
|    `broadcast()`    | 全量复制到所有任务 |    高    |     -      |         分发配置、字典表         |
|     `global()`      |  全部定向到任务 0  |    低    |     差     |          全局聚合、调试          |
| `partitionCustom()` |     自定义规则     |   可控   |    可控    |         特殊业务分区需求         |

#### 5.3.5 分流

**核心作用**



**关键特性**



**适用场景**



**示例代码**

```java

```

**输出特点**

#### 5.3.6 基本合流操作

**核心作用**



**关键特性**



**适用场景**



**示例代码**

```java

```

**输出特点**

### 5.4 输出算子（Sink）



## 第6章 Flink中的时间和窗口



## 第7章 处理函数



## 第8章 状态管理



## 第9章 容错机制



## 第10章 Flink SQL

