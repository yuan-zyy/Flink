package com.zyy.flink.architecture.parallelism;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Parallelism01 {

    public static void main(String[] args) {
        // 1. 环境准备
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration config = new Configuration();
        config.set(RestOptions.PORT, 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        env.setParallelism(3);
        // 2. 从指定的网络端口读取数据
        DataStreamSource<String> ds = env.socketTextStream("localhost", 6666);
        // 3. 对读取的数据进行扁平化处理 --- 向下游传递的是一个个单词而不是二元组
        SingleOutputStreamOperator<String> soso = ds.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> collector) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    collector.collect(word);
                }
            }
        }).setParallelism(4);
        // 4. 对流中的数据进行类型转换 String -> Tuple2<String, Long>
        SingleOutputStreamOperator<Tuple2<String, Long>> result = soso.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String word) throws Exception {
                return Tuple2.of(word, 1L);
            }
        });
        // 5. 按照单词进行分组
        KeyedStream<Tuple2<String, Long>, Tuple> ks = result.keyBy(0);
        // 6. 求和
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = ks.sum(1);
        // 7. 打印
        sum.print();
        // 8. 提交作业
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
