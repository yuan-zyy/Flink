package com.demo.example02.core.xml;

import com.demo.example02.core.config.JobModel;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import java.io.File;

public class JobXmlParser {
    private static final XmlMapper xmlMapper = new XmlMapper();

    public static JobModel parse(String filePath) throws Exception {
        File file = new File(filePath);
        if (!file.exists()) {
            throw new RuntimeException("配置文件不存在: " + filePath);
        }
        return xmlMapper.readValue(file, JobModel.class);
    }
}
