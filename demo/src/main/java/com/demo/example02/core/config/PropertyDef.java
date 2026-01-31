package com.demo.example02.core.config;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

// 新增 PropertyDef.java
public class PropertyDef {

    @JacksonXmlProperty(isAttribute = true)
    public String name;

    @JacksonXmlProperty(isAttribute = true)
    public String value;

}