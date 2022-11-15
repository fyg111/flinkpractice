package com.flinkpractice.flinkdemo.transform;

import com.flinkpractice.flinkdemo.pojo.UserInfo;
import com.flinkpractice.flinkdemo.pojo.UserToFrind;
import com.google.gson.Gson;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * {"uid": 1, "gender": "male" , "name" : "ua" , "friends": [{"fid": 1 , "name" : "cc"} ,{"fid": 3 , "name" : "bb"}]}
 * {"uid": 2, "gender": "male" , "name" : "ub" , "friends": [{"fid": 2 , "name" : "aa"} , {"fid": 3 , "name" : "bb"}]}
 * {"uid": 3, "gender": "female" , "name" : "uc" , "friends": [{"fid": 2 , "name" : "aa"}]}
 * {"uid": 4, "gender": "female" , "name" : "ud" , "friends": [{"fid": 3 , "name" : "bb"}]}
 * {"uid": 5, "gender": "male" , "name" : "ue" , "friends": [{"fid": 1 , "name" : "cc"} ,{"fid": 3 , "name" : "bb"}]}
 * {"uid": 6, "gender": "male" , "name" : "uf" , "friends": [{"fid": 2 , "name" : "aa"} ,{"fid": 3 , "name" : "bb"},{"fid": 1 , "name" : "cc"} ]}
 */

public class TransformationOpration {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> streamSource = env.readTextFile("data/transformpractise");

        /**
         * 把json字符串转成UserInfo
         */
        SingleOutputStreamOperator<UserInfo> map = streamSource.map(json -> new Gson().fromJson(json, UserInfo.class));


        /**
         * 过滤掉好友超过3个的用户
         */
        SingleOutputStreamOperator<UserInfo> filter = map.filter(userinfo -> userinfo.getFriends().size() <= 3);

        /**
         * 取出用户的好友
         * {"uid": 2, "gender": "male" , "name" : "ub" , "friends": [{"fid": 2 , "name" : "aa"} , {"fid": 3 , "name" : "bb"}]}
         *  -》
         *  {"uid": 2, "gender": "male" , "name" : "ub" , "fid": 2 , "name" : "aa"}
         *  {"uid": 2, "gender": "male" , "name" : "ub" , "fid": 3 , "name" : "bb"}
         */
        SingleOutputStreamOperator<String> flatMap = filter.flatMap(new FlatMapFunction<UserInfo, String>() {
            @Override
            public void flatMap(UserInfo userInfo, Collector<String> collector) throws Exception {
                userInfo.getFriends()
                        .forEach(friendInfo -> collector.collect(
                                new Gson().toJson(
                                        new UserToFrind(
                                                userInfo.getUid(),
                                                userInfo.getGender(),
                                                userInfo.getName(),
                                                friendInfo.getFid(),
                                                friendInfo.getName()
                                        )
                                )
                        ));
            }
        });

//        flatMap.print();


        SingleOutputStreamOperator<UserToFrind> flatMap1 = filter.flatMap(new FlatMapFunction<UserInfo, UserToFrind>() {
            @Override
            public void flatMap(UserInfo userInfo, Collector<UserToFrind> collector) throws Exception {
                userInfo.getFriends()
                        .forEach(friendInfo -> collector.collect(
                                        new UserToFrind(
                                                userInfo.getUid(),
                                                userInfo.getGender(),
                                                userInfo.getName(),
                                                friendInfo.getFid(),
                                                friendInfo.getName()
                                        )
                        ));
            }
        });

        /**
         *keyBy算子
         *  对上一步结果，按用户性别分组
         *
         *  滚动聚合算子（只在 keyedStream 上调用），min 、 minBy 、 max 、 maxBy 、 reduce
         *
         *  min 和 max 只会取其他字段的第一条
         *
         *  minBy 和 maxBy 其他字段会取当前条
         */
        KeyedStream<Tuple2<String, Integer>, String> keyBy = flatMap1.map(userToFrind -> Tuple2.of(userToFrind.getGender(), 1))
                .returns(new TypeHint<Tuple2<String, Integer>>() {
                })
                .keyBy(t -> t.f0);

        // 各性别用户的好友总数
        keyBy.sum(1)
//                .print()
        ;

        /**
         * 这两个算子都是求最小值；min 和 minBy 的区别在于：
         *
         *  min 的返回值，最小值字段以外，其他字段是第一条输入数据的值；
         *
         *  minBy 返回值，就是最小值字段所在的那条数据；
         *
         * 底层原理：滚动更新时是更新一个字段，还是更新整条数据的区别
         */
        // 各性别用户好友的最大值
        filter.map(userInfo -> Tuple4.of(
                userInfo.getUid(),
                userInfo.getGender(),
                userInfo.getName(),
                userInfo.getFriends().size()
        )).returns(new TypeHint<Tuple4<String, String, String, Integer>>() {
        }).keyBy(t -> t.f1)
                        .max(3).print();

        env.execute();
    }

}
