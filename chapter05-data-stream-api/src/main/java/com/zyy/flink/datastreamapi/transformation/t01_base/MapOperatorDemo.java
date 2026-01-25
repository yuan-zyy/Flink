package com.zyy.flink.datastreamapi.transformation.t01_base;

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
        map.print("Map:");

        // 5. 执行任务
        env.execute();
    }
}
