package com.zyy.flink.datastreamapi.transformation.t02_agg;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// 自定义富函数类：实现 RichMapFunction 接口（输入 Integer，输出 String）
class MyRichMapFunction extends RichMapFunction<Integer, String> {
    // 模拟：需要初始化的资源（如数据库连接、配置对象）
    private String taskName;
    private int parallelism;
    private int taskId;

    /**
     * 生命周期方法1: open() - 任务启动时调用（每个并行示例仅调用一次）
     * @param parameters 配置参数
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        // 调用父类方法
        super.open(parameters);

        // 1. 获取运行时上下文信息
        this.taskName = getRuntimeContext().getTaskName();
        this.parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
        this.taskId = getRuntimeContext().getIndexOfThisSubtask();

        // 2. 模拟初始化资源（如加载配置、创建数据库连接）
        System.out.println("===== 任务 " + taskId + " 初始化完成 ======");
    }

    /**
     * 核心方法：map() - 处理每个元素时调用
     * @param value 输入元素
     * @return 输出元素
     * @throws Exception
     */
    @Override
    public String map(Integer value) throws Exception {
        // 转换逻辑：拼接数值和运行时信息
        return String.format(
                "作业：%s | 并行度：%d | 任务ID：%d | 输入只：%d | 转换后值：%d",
                taskName, parallelism, taskId, value, value * 3
        );
    }

    /**
     * 生命周期方法2: close() - 任务停止时调用（每个并行示例仅调用一次）
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        // 模拟清理资源（如关闭数据库连接、释放内存）
        System.out.println("===== 任务 " + taskId + " 资源清理完成 ======");
        super.close();
    }
}

public class UdfRichFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为 2， 便于观察不同任务实例的运行时信息
        env.setParallelism(2);

        // 输入数据流
        DataStreamSource<Integer> inputStream = env.fromElements(1, 2, 3, 4, 5);

        // 传入自定义的富函数实例
        DataStream<String> stringDataStream = inputStream.map(new MyRichMapFunction());

        stringDataStream.print("富函数 UDF 输出");
        env.execute();
    }
}
