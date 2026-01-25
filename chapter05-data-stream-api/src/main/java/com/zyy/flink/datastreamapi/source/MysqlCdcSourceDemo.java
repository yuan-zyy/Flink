package com.zyy.flink.datastreamapi.source;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MysqlCdcSourceDemo {

    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // MySQL CDC Source 目前不支持并行读取（单并行度）

        // 2. 构建 MySQL CDC Source
        MySqlSource<String> mySqlCdcSource = MySqlSource.<String>builder()
                .hostname("192.168.1.1")    // MySQL 地址
                .port(3306)                 // MySQL 端口
                .username("root")           // MySQL 用户名
                .password("<PASSWORD>")     // MySQL 密码
                .databaseList("test")       // 要监控的数据库
                .tableList("test.user")     // 要监控的表（格式：数据库.表）
                .deserializer(new JsonDebeziumDeserializationSchema())  // 反序列为 JSON 字符串
                .build();

        // 3. 从 CDC Source 创建 DataStream
        DataStreamSource<String> cdcStream = env.fromSource(mySqlCdcSource, WatermarkStrategy.noWatermarks(), "MySQL CDC Source");

        // 4. 打印输出
        cdcStream.print();

        // 5. 执行作业
        env.execute("MysqlCdcSourceDemo");
    }

}
