package com.demo.example02.core.config;

// 类型映射定义：用于将别名（如 T_Order）映射到具体的 Class 路径
public class TypeDef {
    public String id;     // 别名，如 T_Order
    public String clazz;  // 全类名，如 com.demo.model.OrderEvent
}