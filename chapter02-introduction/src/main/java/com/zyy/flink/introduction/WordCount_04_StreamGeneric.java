package com.zyy.flink.introduction;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCount_04_StreamGeneric {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境（本地环境，集群运行时会自动配置）
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度（本地运行方便查看结果）
        env.setParallelism(1);

        // 2. 读取数据
        String inputPath = "E:\\Idea\\Idea_Study\\Flink\\chapter02-introduction\\input\\word.txt";
        DataStreamSource<String> fileStream = env.readTextFile(inputPath);
        // 3. 数据处理: 切分单词 -> 组装元组
        fileStream.flatMap((String lineVal, Collector<Tuple2<String, Integer>> collator) -> {
            String[] words = lineVal.split(" ");
            for (String word : words) {
                Tuple2<String, Integer> tuple2 = Tuple2.of(word, 1);
                collator.collect(tuple2);
            }
        })
//                .returns(new TypeHint<Tuple2<String, Integer>>() {})
                // 注意: 在使用 lamba 表达式时，因为泛型擦除问题，需要显式的指定返回的类型
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(e -> e.f0)
                .sum(1)
                .print();
        // 7. 执行
        env.execute();
    }

}
