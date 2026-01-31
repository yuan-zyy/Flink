package com.demo.example01;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.util.Map;

// 具体实现 2: 转换
public class MapOp implements CustomOperator {
    @Override
    public SingleOutputStreamOperator<String> apply(DataStream<?> input, Map<String, Object> params) {
        String prefix = (String) params.getOrDefault("prefix", "val");
        return ((DataStream<Integer>) input).map(x -> prefix + ":" + x);
    }
}