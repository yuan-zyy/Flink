package com.demo.example02.core.config;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class SourceDef {
    public String id;
    public String clazz;

    // 关键修复：使用 List 配合注解映射嵌套的 <properties><property/></properties>
    @JacksonXmlElementWrapper(localName = "properties")
    @JacksonXmlProperty(localName = "property")
    public List<PropertyDef> propertyList;

    // 辅助方法：转换为有序 Map，确保顺序与 XML 一致，供有参构造器使用
    public Map<String, String> getPropertiesMap() {
        Map<String, String> map = new LinkedHashMap<>();
        if (propertyList != null) {
            for (PropertyDef p : propertyList) {
                map.put(p.name, p.value);
            }
        }
        return map;
    }
}