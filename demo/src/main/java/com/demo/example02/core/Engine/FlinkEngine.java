package com.demo.example02.core.Engine;

import com.demo.example02.core.config.*;
import com.demo.example02.core.factory.BeanFactory;
import com.demo.example02.core.registry.TypeRegistry;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;

public class FlinkEngine {
    private Map<String, DataStream<Object>> streamRegistry = new HashMap<>();

    public void build(JobModel model) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1. 构建 Source
        for (SourceDef sd : model.sources) {
            DataStream<Object> src = env.addSource((SourceFunction<Object>) BeanFactory.createAndInject(sd.clazz, sd.getPropertiesMap()));
            streamRegistry.put(sd.id, src);
        }

        // 2. 构建 Transform (处理合流、分流、窗口)
        for (TransformDef td : model.transforms) {
            DataStream<Object> input = resolveInputs(td.inputs);
            TypeInformation<Object> outType = TypeRegistry.get(td.outType);
            Object handler = BeanFactory.createAndInject(td.clazz, td.properties);

            SingleOutputStreamOperator<Object> out;
            if (td.window != null) { // 窗口逻辑
                out = input.keyBy(obj -> BeanUtils.getProperty(obj, td.keyBy))
                        .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                        .aggregate((AggregateFunction) handler).returns(outType);
            } else if (td.outputs != null) { // SideOutput 分流
                out = input.process((ProcessFunction) handler).returns(outType);
                for (OutputDef od : td.outputs) {
                    streamRegistry.put(od.id, out.getSideOutput(new OutputTag<Object>(od.tag){}));
                }
            } else { // 普通 Map
                out = input.map((MapFunction) handler).returns(outType);
            }
            streamRegistry.put(td.id, out.name(td.id).uid(td.id));
        }

        // 3. 构建 Sink
        for (SinkDef sk : model.sinks) {
            streamRegistry.get(sk.input).addSink((SinkFunction) BeanFactory.createAndInject(sk.clazz, sk.properties));
        }

        env.execute(model.name);
    }

    private DataStream<Object> resolveInputs(String inputIds) {
        String[] ids = inputIds.split(",");
        DataStream<Object> first = streamRegistry.get(ids[0]);
        if (ids.length == 1) return first;

        DataStream<Object>[] others = new DataStream[ids.length - 1];
        for (int i = 1; i < ids.length; i++) others[i-1] = streamRegistry.get(ids[i]);
        return first.union(others);
    }
}