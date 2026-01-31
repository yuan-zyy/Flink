package com.demo.example02;

import com.demo.example02.core.Engine.FlinkEngine;
import com.demo.example02.core.config.JobModel;
import com.demo.example02.core.xml.JobXmlParser;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MainApp {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        try {
            // 第一步：解析 XMLmodel = {JobModel@1880}
            JobModel model = JobXmlParser.parse("H:\\ProjectSource\\Idea\\Flink\\demo\\src\\main\\resources\\job-config.xml");
            System.out.println("成功加载作业: " + model.name);

            // 第二步：初始化引擎并构建拓扑
            FlinkEngine engine = new FlinkEngine();
            engine.build(model);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}