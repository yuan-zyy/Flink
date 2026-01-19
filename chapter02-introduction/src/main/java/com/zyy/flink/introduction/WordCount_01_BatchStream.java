package com.zyy.flink.introduction;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCount_01_BatchStream {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 2. 读取数据
        String inputPath = "E:\\Idea\\Idea_Study\\Flink\\chapter02-introduction\\input\\word.txt";
        DataSource<String> dataSource = env.readTextFile(inputPath);
        // 3. 将读取数据进行扁平化处理，封装为二元组对象 Tuple2<单词，1L>
        FlatMapOperator<String, Tuple2<String, Integer>> wordOneOperator = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String lineVal, Collector<Tuple2<String, Integer>> collector) throws Exception {
                // 分割字符串数据
                String[] words = lineVal.split(" ");
                for (String word : words) {
                    Tuple2<String, Integer> tuple2 = Tuple2.of(word, 1);
                    collector.collect(tuple2);
                }
            }
        });
        // 4. 按照单词进行分组
        UnsortedGrouping<Tuple2<String, Integer>> group = wordOneOperator.groupBy(0);
        // 5. 聚合计算
        AggregateOperator<Tuple2<String, Integer>> sum = group.sum(1);
        // 6. 打印结果
        sum.print();
    }

}
