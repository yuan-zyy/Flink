package com.zyy.flink.datastreamapi.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Source04Kafka {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从 Kafka 读取数据
        KafkaSource<String> suorce = KafkaSource.<String>builder()
                .setBootstrapServers("192.168.1.1:9092")
                .setTopics("test")
                .setGroupId("test")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        env.fromSource(suorce, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .print();
        env.execute();
    }

}
