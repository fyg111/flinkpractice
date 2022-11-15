package com.flinkpractice.flinkdemo.source.customsource;

import com.flinkpractice.flinkdemo.pojo.EventLog;
import com.flinkpractice.flinkdemo.source.customsource.noparallel.RichSourceFunctionDemo;
import com.flinkpractice.flinkdemo.source.customsource.parallel.RIchSourceFunctionParallelDemo;
import com.google.gson.Gson;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * RichSourceFunction SourceFunction 只能是单并行度
 *
 */

public class Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /*DataStreamSource<EventLog> dataStreamSource = env.addSource(new RichSourceFunctionDemo())
                .setParallelism(5) // 运行报错    The parallelism of non parallel operator must be 1.
                ;*/
        DataStreamSource<EventLog> dataStreamSource = env.addSource(new RIchSourceFunctionParallelDemo())
         .setParallelism(5)
        ;


        dataStreamSource.map(info -> new Gson().toJson(info)).print();
        env.execute();
    }
}
