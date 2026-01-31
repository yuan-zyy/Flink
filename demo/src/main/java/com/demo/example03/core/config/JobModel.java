package com.demo.example03.core.config;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

import java.util.List;

@JacksonXmlRootElement(localName = "flink-job")
public class JobModel {
    @JacksonXmlProperty(isAttribute = true)
    public String name;

//    @JacksonXmlElementWrapper(localName = "data-types")
//    @JacksonXmlProperty(localName = "type")
//    public List<TypeDef> dataTypes;

//    @JacksonXmlElementWrapper(localName = "sources")
//    @JacksonXmlProperty(localName = "source")
//    public List<SourceDef> sources;

}