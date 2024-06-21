package com.ash.flink.tfm;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class ReduceTest {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromCollection(Arrays.asList(1, 2, 3, 4, 5));

        env.fromCollection(Arrays.asList("a", "b", "c"));


    }

}
