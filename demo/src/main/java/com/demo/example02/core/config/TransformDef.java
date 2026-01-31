package com.demo.example02.core.config;

import java.util.*;

/**
 * 算子转换定义模型
 * 对应 XML 中的 <transform> 标签
 */
public class TransformDef {
    // 1. 基础标识
    public String id;           // 算子 ID，强制绑定为 Flink 的 uid 和 name
    public String inputs;       // 上游输入 ID，支持逗号分隔（如 "src1,src2" 自动触发 union）
    public String clazz;        // 业务实现类全路径（实现 Map/Process/Aggregate 等接口）

    // 2. 类型定义 (对应 TypeDef 中的 ID)
    public String inType;       // 输入数据类型 ID
    public String outType;      // 输出数据类型 ID

    // 3. 属性注入
    public Map<String, String> properties = new HashMap<>(); // 动态注入业务类的参数

    // 4. 资源控制
    public int parallelism = 0; // 局部并行度覆盖
    public String slotGroup;    // 槽共享组
    public String chaining = "default"; // 算子链控制: start, disable, default

    // 5. 窗口与聚合 (可选)
    public String keyBy;        // 分组字段名
    public WindowDef window;    // 窗口配置模型

    // 6. 侧输出流 (Side Output)
    public List<OutputDef> outputs;

    // 快捷判断方法
    public boolean isWindow() { return window != null; }
    public boolean isMultiInput() { return inputs != null && inputs.contains(","); }
}