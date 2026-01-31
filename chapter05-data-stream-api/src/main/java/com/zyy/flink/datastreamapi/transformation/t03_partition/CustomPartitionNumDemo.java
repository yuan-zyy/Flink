package com.zyy.flink.datastreamapi.transformation.t03_partition;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CustomPartitionNumDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        DataStream<Integer> inputStream = env.fromElements(1, 5, 8, 2, 6, 9, 3, 4, 7);

        // 自定义分区：按数据范围分发
        DataStream<Integer> userDataStream = inputStream.partitionCustom(
                // 第一个参数：自定义 Partitioner 实现类
                new Partitioner<Integer>() {
                    /**
                     *
                     * @param key 用于分区的关键字段
                     * @param numPartitions 下游算子的并行任务数量
                     * @return 下游并行任务的编号（0 ~ numPartitions-1），指定数据分发到哪个任务
                     */
                    @Override
                    public int partition(Integer key, int numPartitions) {
                        // 自定义分区规则
                        int partitions = key % numPartitions;
                        System.out.println("key: " + key + ", numPartitions: " + numPartitions + ", key % numPartitions: " + partitions);
                        return partitions;
                    }
                },
                // 第二个参数：指定分区键（这里使用用户的ID作为分区键）
                new KeySelector<Integer, Integer>() {
                    @Override
                    public Integer getKey(Integer num) throws Exception {
                        return num;
                    }
                }
        );

        userDataStream.print("自定义分区结果");
        env.execute("Flink Custom Partition Demo");
    }

    @Data
    @AllArgsConstructor
    public static class User {
        public int id;
        public String name;
    }

}

