package com.demo;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.util.Map;

// 具体实现 1: 过滤
public class FilterOp implements CustomOperator {
    @Override
    public SingleOutputStreamOperator<Integer> apply(DataStream<?> input, Map<String, Object> params) {
        int min = (int) params.getOrDefault("min", 0);
        return ((DataStream<Integer>) input).filter(x -> x >= min);
    }
}