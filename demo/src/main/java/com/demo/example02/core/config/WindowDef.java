package com.demo.example02.core.config;

public class WindowDef {
    public String type;   // tumbling, sliding, session
    public String size;   // 10s, 5m, 1h
    public String slide;  // 仅滑动窗口使用
    public String gap;    // 仅会话窗口使用
}