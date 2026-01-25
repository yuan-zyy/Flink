package com.zyy.flink.datastreamapi.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FileSourceDemo {
    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 构建 File Source（读取文本文件，支持本地文件/HDFS）
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(
                new TextLineInputFormat(),  // 输入格式：按行读取文本
                new Path("data/input/demo.txt") // 文件路径
        ).build();

        // 3. 构建数据流
        DataStreamSource<String> fileStream = env.fromSource(
                fileSource,
                WatermarkStrategy.noWatermarks(),
                "File Source Demo"
        );

        // 4. 打印输出
        fileStream.print("FileStream:");

        // 5. 执行作业
        env.execute("FileSourceDemo");
    }
}
