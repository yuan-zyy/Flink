package com.demo.example02.core.config;

// 全局配置定义
public class SettingsDef {
    public int defaultParallelism = 1;
    public long checkpointInterval = 60000; // 默认1分钟
    public String checkpointMode = "EXACTLY_ONCE";
}