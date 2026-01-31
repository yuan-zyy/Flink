package com.demo.example01;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.Arrays;
import java.util.List;

public class ConfigurableFlinkJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1. 模拟配置文件内容 (实际生产中可以从本地文件或 Apollo 加载)
        String jsonConfig = "[" +
                "{\"id\":\"f1\", \"className\":\"com.demo.example01.FilterOp\", \"parallelism\":2, \"chainingStrategy\":\"DEFAULT\", \"params\":{\"min\":10}}," +
                "{\"id\":\"m1\", \"className\":\"com.demo.example01.MapOp\", \"parallelism\":2, \"chainingStrategy\":\"START_NEW\", \"params\":{\"prefix\":\"A\"}}" +
                "]";

        ObjectMapper mapper = new ObjectMapper();
        List<OpConfig> configs = Arrays.asList(mapper.readValue(jsonConfig, OpConfig[].class));

        // 2. 初始 Source
        DataStream<Integer> source = env.fromElements(1, 15, 8, 20, 5, 30).name("MockSource");
        DataStream<?> currentStream = source;

        // 3. 动态构建算子链
        for (OpConfig conf : configs) {
            CustomOperator op = (CustomOperator) Class.forName(conf.className).getDeclaredConstructor().newInstance();
            SingleOutputStreamOperator<?> nextStream = op.apply(currentStream, conf.params);

            // 体现级联关系的代码点
            nextStream.name(conf.id).setParallelism(conf.parallelism);

            if ("START_NEW".equalsIgnoreCase(conf.chainingStrategy)) {
                nextStream.startNewChain();
            } else if ("DISABLE".equalsIgnoreCase(conf.chainingStrategy)) {
                nextStream.disableChaining();
            }

            currentStream = nextStream;
        }

        currentStream.print();

        // 打印 Plan 以查看级联效果
        System.out.println(env.getExecutionPlan());
        env.execute("Configurable Chaining Job");
    }
}