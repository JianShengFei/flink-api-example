package com.ash.flink.tfm;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class IterateTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketDs = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<Integer> convertInt = socketDs.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) throws Exception {
                return Integer.parseInt(value);
            }
        });

        IterativeStream<Integer> iterateDs = convertInt.iterate();
        SingleOutputStreamOperator<Integer> iterOperator = iterateDs.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                return value - 1;
            }
        });

        SingleOutputStreamOperator<Integer> continueOperator = iterOperator.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer value) throws Exception {
                return value > 0;
            }
        });


        iterateDs.closeWith(continueOperator);
        iterateDs.print();
        env.execute();


    }


}
