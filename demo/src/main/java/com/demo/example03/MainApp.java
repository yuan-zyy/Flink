package com.demo.example03;

import com.demo.example03.core.config.JobModel;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

public class MainApp {
    public static void main(String[] args) {
        try {
            // 第一步：解析 XMLmodel = {JobModel@1880}
            JobModel model = new XmlMapper().readValue("H:\\ProjectSource\\Idea\\Flink\\demo\\src\\main\\resources\\job-config.xml",
                    JobModel.class);
            System.out.println("成功加载作业: " + model.name);

            // 第二步：初始化引擎并构建拓扑

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}