package com.zyy.flink.datastreamapi.source;

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
