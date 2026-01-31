package com.demo.example02.core.config;

import java.util.Map;

// 数据汇定义
public class SinkDef {
    public String id;
    public String input; // 对应上游算子的 ID
    public String clazz;
    public Map<String, String> properties;
}