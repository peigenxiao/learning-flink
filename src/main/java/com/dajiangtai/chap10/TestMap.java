package com.dajiangtai.chap10;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: 🐟lifei🐟
 * @Date: 2018/12/17 下午10:06
 */
public class TestMap {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Long> input=env.generateSequence(0,10);

        DataStream plusOne=input.map(new MapFunction<Long, Long>() {

            @Override
            public Long map(Long value) throws Exception {
                System.out.println("--------------------"+value);
                return value+1;
            }
        });

        plusOne.print();

        env.execute();
    }
}
