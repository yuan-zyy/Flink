package com.demo;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.util.Map;

public interface CustomOperator {

    /**
     * @param input 输入流
     * @param params 配置文件中的自定义参数
     * @return 处理后的流
     */
    SingleOutputStreamOperator<?> apply(DataStream<?> input, Map<String, Object> params);

}
