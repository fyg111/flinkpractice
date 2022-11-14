package com.flinkpractice.flinkdemo.day01

import org.apache.flink.streaming.api.scala.{createTypeInformation, StreamExecutionEnvironment}

object WordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)
    env.socketTextStream("192.168.9.13", 9999)
      .flatMap(x =>x.split("\\s+").map((_, 1)))
      .keyBy(_._1)
      .sum(1)
      .print()

    env.execute()
  }

}
