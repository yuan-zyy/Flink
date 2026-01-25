package com.zyy.flink.datastreamapi.source;

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
