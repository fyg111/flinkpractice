package com.flinkpractice.flinkdemo.source.fromcollection;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.LongValueSequenceIterator;

public class FromParallelCollection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<LongValue> fromParallelCollection = env.fromParallelCollection(new LongValueSequenceIterator(1l, 10l), TypeInformation.of(LongValue.class))
                .setParallelism(5);

        fromParallelCollection.map(x -> x.getValue() + 1).print();

        env.execute();
    }
}
