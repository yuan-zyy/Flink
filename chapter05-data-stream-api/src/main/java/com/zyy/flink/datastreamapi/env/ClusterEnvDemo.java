package com.zyy.flink.datastreamapi.env;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;
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