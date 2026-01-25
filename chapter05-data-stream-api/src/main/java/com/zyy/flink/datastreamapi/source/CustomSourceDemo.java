package com.zyy.flink.datastreamapi.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class CustomSourceDemo {
    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 自定义数据源
        DataStreamSource<Long> myCustomSource = env.addSource(new MyCustomSource(), "MyCustomSource");

        // 3. 打印输出
        myCustomSource.print("Custom Souorce:");

        // 4. 执行作业
        env.execute("CustomSourceDemo");
    }

    // 自定义 Source: 实现 RichParallelSourceFunction
    public static class MyCustomSource extends RichParallelSourceFunction<Long> {
        // 标记是否继续运行（用于取消作业时终止循环）
        private volatile boolean isRunning = true;
        // 递增数字
        private long count = 0;

        @Override
        public void run(SourceContext<Long> ctx) throws Exception {
            while (isRunning) {
                // 加锁写入数据（保证并发场景下的数据一致性）
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(count); // 发送数据到下游算子
                    count++;
                }
                // 每秒生产一个数据
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            // 取消作业时，将标记设置为 false，终止 run() 方法中的循环
            isRunning = false;
        }
    }
}
