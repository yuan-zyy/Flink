package com.zyy.flink.architecture.parallelism;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class Parallelism_Demo {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 执行环境级别：设置当前作业默认并行度为 2
        env.setParallelism(2);

        // 3. 算子级别：为 source 算子设置并行度 3（优先级高于环境级别）
        env.addSource(new CustomSource())
                .setParallelism(1)  // Source 并行度 3 这是设置为3会报错
                .map(value -> value + " processed")
                .setParallelism(4)  // Map 并行度 4
                .print()            // Print 算子使用环境默认并行度 2
                .setParallelism(2);

        // 执行作业
        env.execute();
    }

    // 自定义简单 source
    static class CustomSource implements SourceFunction<String> {
        private boolean isRuning = true;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            int i = 0;
            while (isRuning) {
                ctx.collect("data-" + i++);
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            isRuning = false;
        }
    }

}
