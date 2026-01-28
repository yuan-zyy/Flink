package com.zyy.flink.datastreamapi.partition;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
