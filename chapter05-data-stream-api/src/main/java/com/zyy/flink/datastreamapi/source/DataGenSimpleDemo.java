package com.zyy.flink.datastreamapi.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataGenSimpleDemo {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataGeneratorSource<String> generatorSource = new DataGeneratorSource<>(new GeneratorFunction<Long, String>() {
            @Override
            public String map(Long value) throws Exception {
                return "数据 -> " + value;
            }
        },
                100,
                RateLimiterStrategy.perSecond(10),
                TypeInformation.of(String.class));

        DataStreamSource<String> dataStreamSource = env.fromSource(
                generatorSource,
                WatermarkStrategy.noWatermarks(),
                "DataGen Source"
        );

        dataStreamSource.print();
        env.execute();
    }
}
