package com.zyy.flink.datastreamapi.transformation.t01_base;

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
