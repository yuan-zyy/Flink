package com.demo.example02.core.registry;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.HashMap;
import java.util.Map;

public class TypeRegistry {
    private static final Map<String, TypeInformation<Object>> registry = new HashMap<>();
    public static void register(String id, String clazz) throws Exception {
        registry.put(id, TypeInformation.of((Class<Object>) Class.forName(clazz)));
    }
    public static TypeInformation<Object> get(String id) { return registry.get(id); }
}