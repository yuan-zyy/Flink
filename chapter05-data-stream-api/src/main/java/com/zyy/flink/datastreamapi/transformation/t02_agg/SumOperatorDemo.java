package com.zyy.flink.datastreamapi.transformation.t02_agg;

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
