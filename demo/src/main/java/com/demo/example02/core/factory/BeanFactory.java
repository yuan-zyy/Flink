package com.demo.example02.core.factory;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.ConvertUtils;
import java.lang.reflect.Constructor;
import java.util.*;

public class BeanFactory {
    public static Object createAndInject(String className, Map<String, String> properties) throws Exception {
        Class<?> clazz = Class.forName(className);
        int propSize = (properties == null) ? 0 : properties.size();

        // 1. 获取所有构造函数
        Constructor<?>[] constructors = clazz.getConstructors();

        // 2. 寻找参数数量匹配的构造器 (针对 Flink 原生算子)
        Constructor<?> bestMatch = Arrays.stream(constructors)
                .filter(c -> c.getParameterCount() == propSize)
                .findFirst().orElse(null);

        if (bestMatch != null && propSize > 0) {
            // 自动将 String 转换为构造器需要的类型 (int, long 等)
            Object[] args = new Object[propSize];
            Class<?>[] paramTypes = bestMatch.getParameterTypes();
            Object[] vals = properties.values().toArray();
            for (int i = 0; i < paramTypes.length; i++) {
                args[i] = org.apache.commons.beanutils.ConvertUtils.convert(vals[i].toString(), paramTypes[i]);
            }
            return bestMatch.newInstance(args);
        }

        // 3. 兜底方案：无参构造 + BeanUtils 注入 (针对自定义 POJO 算子)
        Object instance = clazz.getDeclaredConstructor().newInstance();
        if (properties != null) BeanUtils.populate(instance, properties);
        return instance;
    }
}


//
//public class BeanFactory {
//
//    public static Object createAndInject(String className, Map<String, String> properties) throws Exception {
//        Class<?> clazz = Class.forName(className);
//        Constructor<?>[] constructors = clazz.getConstructors();
//        int propSize = (properties == null) ? 0 : properties.size();
//
//        // 1. 策略：寻找参数数量与 XML properties 数量完全匹配的构造函数
//        Constructor<?> bestMatch = Arrays.stream(constructors)
//                .filter(c -> c.getParameterCount() == propSize)
//                .findFirst()
//                .orElse(null);
//
//        // 2. 场景 A：匹配到有参构造器 (如 Flink 原生 SocketSource)
//        if (bestMatch != null && propSize > 0) {
//            Object[] args = resolveArgs(bestMatch, properties);
//            return bestMatch.newInstance(args);
//        }
//
//        // 3. 场景 B：兜底使用无参构造器 + Setter 注入 (针对自定义 POJO)
//        Object instance = clazz.getDeclaredConstructor().newInstance();
//        if (properties != null) {
//            BeanUtils.populate(instance, properties);
//        }
//        return instance;
//    }
//
//    private static Object[] resolveArgs(Constructor<?> constructor, Map<String, String> properties) {
//        Class<?>[] paramTypes = constructor.getParameterTypes();
//        Object[] args = new Object[paramTypes.length];
//        Object[] rawValues = properties.values().toArray();
//
//        for (int i = 0; i < paramTypes.length; i++) {
//            // ConvertUtils 自动处理 String 到目标类型的转换
//            args[i] = ConvertUtils.convert(rawValues[i].toString(), paramTypes[i]);
//        }
//        return args;
//    }
//}
//
//public class BeanFactory {
//    /**
//     * 创建算子实例并注入配置参数
//     * @param className 算子全类名
//     * @param properties XML中定义的键值对
//     */
//    public static <T> T createAndInject(String className, Map<String, String> properties) throws Exception {
//        // 1. 反射实例化
//        Class<?> clazz = Class.forName(className);
//        T instance = (T) clazz.getDeclaredConstructor().newInstance();
//
//        // 2. 自动注入 (调用对应的 setXxx 方法)
//        if (properties != null && !properties.isEmpty()) {
//            // BeanUtils 会自动处理 String 到基础类型(int, double, boolean)的转换
//            BeanUtils.populate(instance, properties);
//        }
//        return instance;
//    }
//}

//public class BeanFactory {
//    public static Object createAndInject(String className, Map<String, String> properties) throws Exception {
//        Object obj = Class.forName(className).getDeclaredConstructor().newInstance();
//        if (properties != null) BeanUtils.populate(obj, properties);
//        return obj;
//    }
//}