package com.flinkpractice.flinkdemo.source.fromcollection;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FromCollection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<String> list = Arrays.asList("a", "b", "c");
        DataStreamSource<String> fromCollection = env.fromCollection(list);   // 单并行度算子

        fromCollection.map(String::toUpperCase).print();

        env.execute();
    }
}
