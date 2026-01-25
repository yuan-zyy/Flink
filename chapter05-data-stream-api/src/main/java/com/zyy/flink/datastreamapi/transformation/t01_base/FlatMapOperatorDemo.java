package com.zyy.flink.datastreamapi.transformation.t01_base;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlatMapOperatorDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 输入数据流：两行文本
        DataStream<String> inputStream = env.fromElements("hello world", "hello flink");

        // flatMap算子：将每行文本按空格拆分，输出单个单词
        SingleOutputStreamOperator<String> resultStream = inputStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                // 拆分文本
                String[] words = s.split(" ");
                // 遍历拆分结果，逐个输出
                for (String word : words) {
                    collector.collect(word);
                }
            }
        });

        resultStream.print("flatMap算子输出");
        env.execute("FlatMap Operator Demo");
    }
}
