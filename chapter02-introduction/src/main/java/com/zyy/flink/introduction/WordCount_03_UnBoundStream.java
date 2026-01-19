package com.zyy.flink.introduction;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 无界流处理
 */
public class WordCount_03_UnBoundStream {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 读取数据
        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 9999);
        // 3. 数据处理
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordOneOperator = dataStream
                .flatMap((String lineVal, Collector<Tuple2<String, Integer>> collector) -> {
                    String[] words = lineVal.split(" ");
                    for (String word : words) {}
                }).returns(new TypeHint<Tuple2<String, Integer>>() {});
        // 4. 分组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordOneOperator.keyBy(e -> e.f0);
        // 5. 聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyedStream.sum(1);
        sum.print();
        env.execute();
    }

}
