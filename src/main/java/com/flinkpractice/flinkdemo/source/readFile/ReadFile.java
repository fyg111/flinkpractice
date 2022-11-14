package com.flinkpractice.flinkdemo.source.readFile;

import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

public class ReadFile {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStreamSource = env.readFile(new TextInputFormat(null), "data/wc", FileProcessingMode.PROCESS_CONTINUOUSLY, 2L);
        dataStreamSource.map(String::toUpperCase).print();

        env.execute();
    }
}
