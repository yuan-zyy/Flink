package com.zyy.flink.datastreamapi.env;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RemoteEnvDemo {

    public static void main(String[] args) throws Exception {
        // 创建远程执行环境，指定 JobManager 的地址、端口、并行度、依赖 Jar 包
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(
                "192.168.1.1", // JobManager 主机名/IP
                8081,               // JobManager RPC 端口（默认 8081）
                2,                  // 全局并行度
                "xxx.jar"  // 任务依赖的 jar 包（可选）
        );

        // 后端业务逻辑
        env.readTextFile("hdfs://192.168.1.1:9000/data/input.txt")
                .print();

        // 提交任务（直接提交到指定的远程 JobManager）
        env.execute("Remote Flink Job Demo");
    }

}
