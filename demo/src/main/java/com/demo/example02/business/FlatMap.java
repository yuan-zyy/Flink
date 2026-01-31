package com.demo.example02.business;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class FlatMap implements FlatMapFunction<String, String> {
    @Override
    public void flatMap(String line, Collector<String> collector) throws Exception {
        String[] split = line.split(" ");
        for (String word : split) {
            collector.collect(word);
        }
    }
}
