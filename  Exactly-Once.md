 # Exactly-Once
 
## 一、先搞懂: Exactly-Once 到底是什么？
在分布式流处理中，数据处理的语义分为三个级别，我们先通过对比理解:  

| 语义级别          | 通俗解释                   | 适用场景         |
|---------------|------------------------|--------------|
| At-Most-Once  | 最多一次: 数据可能丢，绝对不会重复     | 非核心场景（如日志采集） |
| At-Least-Once | 至少一次: 数据绝对不丢，但可能重复     | 允许少量重复的场景    |
| Exactly-Once  | 精确一次: 数据既不丢，也不重复，只处理一次 | 金融、订单、计费核心场景 |
简单来说，Exactly-Once 保证：**即使发生机器故障、网络抖动等异常，每条数据最终也只会被计算一次，结果完全准确**

**注意**：Exactly-One 是**计算结果的精准一次**，而非**数据传输的精准一次**（比如Kafka 到 Flink 可能传多次，但Flink最终只算一次）

## 二、Flink 实现 Exactly-Once 的核心原理
Flink 实现 Eaxctly-Once 的核心是 **Checkpoint（检查点）+ 分布式快照 + 两阶段提交（2PC）**，我们拆解成新手能动的逻辑：
### 1. 基础: Checkpoint 与状态快照
- Checkpoint: Flink 定期（可配置）将当前所有算子的**状态**（比如单词计数器的累计值）和**数据处理位置**（比如Kafka的offset）保存到外部存储（如HDFS、RockDB），形成一个**快照**
- 当发生故障时，Flink 会从最近的 Checkpoint 恢复：回滚状态到快照时刻，并重放快照之后的数据流，保证状态和数据处理进度一致

### 2. 关键：两阶段提交（2PC）
Checkpoint 只能保证 Flink 内部状态的一致性，但如果涉及**外部系统写入**（比如将计算结果写入MySql、Kafka），就需要2PC来协调 Flink 和外部系统的事务
- 阶段1（预提交）：Flink 触发 Checkpoint 时，先让所有算子将待写入外部系统的数据**预提交**（比如写入临时表、或标记为带确认），但不真正生效
- 阶段2（确认提交）：当 Flink 确认所有算子的 Checkpoint 都完成后，再通知外部系统**正式提交**数据，如果由任何算子失败，则通知外部系统**回滚**预提交的数据

### 3. 核心组件：TwoPhaseCommitSinkFunction
Flink 为外部系统写入提供了通用的 2PC 实现类 TwoPhaseCommitSinkFunction，只要外部系统支持事务（如 Kafka、MySQL、HBase），就能基于这个类实现 Exactly-Once 写入

## 三、实战示例：Flink + Kafka 实现 Exactly-Once 生产
以「Flink 读取 Kafka 数据，处理后再写入另一个 Kafka Topic」为例，展示如何配置 Exactly-Once：

### 核心依赖（pom.xml 补充）
```xml
<!-- Flink Kafka 连接器 -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-kafka</artifactId>
    <version>1.18.0</version>
</dependency>   
```

### 2. 代码实现（关键配置）
```java
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;

public class FlinkExactlyOnceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // ========== 1. 开启Checkpoint（Exactly-Once的基础） ==========
        // 每5秒触发一次Checkpoint
        env.enableCheckpointing(5000);
        // 设置Checkpoint模式为EXACTLY_ONCE（核心）
        env.getCheckpointConfig().setCheckpointingMode(org.apache.flink.streaming.api.CheckpointingMode.EXACTLY_ONCE);
        // Checkpoint超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 同一时间只允许一个Checkpoint运行
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // Checkpoint失败时，任务是否失败（生产建议设为true）
        env.getCheckpointConfig().setFailOnCheckpointingErrors(true);

        // ========== 2. 读取Kafka数据源（配置offset重置策略） ==========
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("source_topic")
                .setGroupId("flink-exactly-once-group")
                // 从最早的offset开始消费（也可设为LATEST）
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new org.apache.flink.api.common.serialization.SimpleStringSchema())
                .build();

        // ========== 3. 处理数据（这里简单做个转换） ==========
        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .map(data -> "processed_" + data) // 模拟数据处理

        // ========== 4. 写入Kafka（配置Exactly-Once） ==========
                .sinkTo(KafkaSink.<String>builder()
                        .setBootstrapServers("localhost:9092")
                        .setRecordSerializer(KafkaRecordSerializationSchema.<String>builder()
                                .setTopic("sink_topic")
                                .setValueSerializationSchema(new org.apache.flink.api.common.serialization.SimpleStringSchema())
                                .build())
                        // 核心：设置事务模式为EXACTLY_ONCE
                        .setDeliveryGuarantee(org.apache.flink.connector.kafka.sink.KafkaSink.DeliveryGuarantee.EXACTLY_ONCE)
                        // 事务超时时间（必须大于Checkpoint间隔，小于Kafka的transaction.timeout.ms）
                        .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "60000")
                        .build());

        env.execute("Flink Exactly-Once Demo");
    }
}
```
