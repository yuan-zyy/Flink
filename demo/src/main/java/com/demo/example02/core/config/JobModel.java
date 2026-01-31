package com.demo.example02.core.config;

import java.util.List;

import com.fasterxml.jackson.dataformat.xml.annotation.*;

@JacksonXmlRootElement(localName = "flink-job")
public class JobModel {
    @JacksonXmlProperty(isAttribute = true)
    public String name;

    @JacksonXmlProperty(localName = "settings")
    public SettingsDef settings;

    @JacksonXmlElementWrapper(localName = "data-types")
    @JacksonXmlProperty(localName = "type")
    public List<TypeDef> dataTypes;

    @JacksonXmlElementWrapper(localName = "sources")
    @JacksonXmlProperty(localName = "source")
    public List<SourceDef> sources;

    @JacksonXmlElementWrapper(localName = "transforms")
    @JacksonXmlProperty(localName = "transform")
    public List<TransformDef> transforms;

    @JacksonXmlElementWrapper(localName = "sinks")
    @JacksonXmlProperty(localName = "sink")
    public List<SinkDef> sinks;
}
//
//// 全局作业配置
//public class JobModel {
//    public String name;
//
////    public SettingsDef settings;
//    public List<TypeDef> dataTypes;
//
//    public List<SourceDef> sources;
//
//    public List<TransformDef> transforms;
//
//    public List<SinkDef> sinks;
//}
