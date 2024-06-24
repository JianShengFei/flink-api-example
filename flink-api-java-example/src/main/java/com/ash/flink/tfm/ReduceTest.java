package com.ash.flink.tfm;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class ReduceTest {

    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        DataStreamSource<Tuple2<String, Integer>> ds = env.fromCollection(Arrays.asList(
//                Tuple2.of("a", 1),
//                Tuple2.of("a", 2),
//                Tuple2.of("b", 3),
//                Tuple2.of("b", 2),
//                Tuple2.of("c", 5)));
//
//        KeyedStream<Tuple2<String, Integer>, Object> keyedStream = ds.keyBy(t -> t.f0);
//        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = keyedStream.reduce((t1, t2) -> Tuple2.of(t1.f0, t1.f1 + t2.f1));
//
//        reduce.print();
//
//        env.execute();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<String, Integer>> ds = env.fromCollection(Arrays.asList(
                Tuple2.of("a", 1),
                Tuple2.of("b", 2),
                Tuple2.of("c", 3),
                Tuple2.of("a", 4),
                Tuple2.of("b", 5)));
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = ds.keyBy((KeySelector<Tuple2<String, Integer>, String>) tp -> tp.f0);

        keyedStream.reduce((v1, v2) -> Tuple2.of(v1.f0, v1.f1 + v2.f1)).print();
        keyedStream.reduce((ReduceFunction<Tuple2<String, Integer>>) (v1, v2) -> Tuple2.of(v1.f0,v1.f1+v2.f1)).print();
        env.execute();

    }

}
