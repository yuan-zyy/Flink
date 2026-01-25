package com.zyy.flink.datastreamapi.env;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class LocalEnvDemo {

    public static void main(String[] args) throws Exception {
        // 方式1: 创建默认本地之心环境（并行度默认等于当前机器的 CPU 核心数）
        LocalStreamEnvironment env1 = StreamExecutionEnvironment.createLocalEnvironment();

        // 方式2: 指定并行度的本地执行环境（推荐，调试时并行度可控，避免混乱）
        LocalStreamEnvironment env2 = StreamExecutionEnvironment.createLocalEnvironment(2);

        // 方式3: 带配置的本地执行环境（可配置更多参数，如状态后端、内存等）
//        Configuration conf = new Configuration();
//        conf.setInteger("state.backend.rocksdb.memory.off-heap", 1);
//        LocalStreamEnvironment env3 = StreamExecutionEnvironment.createLocalEnvironment(2, conf);

        // 后续业务逻辑（示例：读取本地文件）
        env1.readTextFile("file:///tmp/test.txt")
                .print();

        // 提交作业（本地环境，execute()）会直接触发运行
        env2.execute("Local Flink Job Demo");
    }

}
