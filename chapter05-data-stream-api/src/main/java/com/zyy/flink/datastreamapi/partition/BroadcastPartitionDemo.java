package com.zyy.flink.datastreamapi.partition;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class BroadcastPartitionDemo {
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
}
