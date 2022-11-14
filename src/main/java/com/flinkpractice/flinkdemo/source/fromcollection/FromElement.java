package com.flinkpractice.flinkdemo.source.fromcollection;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FromElement {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> fromElements = env.fromElements(1, 2, 3, 4, 5);  // 单并行度算子

        fromElements.map(i -> i + 10).print();

        env.execute();
    }
}
