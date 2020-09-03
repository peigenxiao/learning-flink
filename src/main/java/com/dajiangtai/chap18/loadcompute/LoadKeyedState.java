package com.dajiangtai.chap18.loadcompute;

import com.dajiangtai.chap18.keyedstate.CountWithKeyedState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: 🐟lifei🐟
 * @Date: 2019/1/22 下午12:18
 */
public class LoadKeyedState {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<Long,Long>> inputStream=env.fromElements(
     // 1, 2, 2, 2, 2, 1, 2, 2, 3, 3, 3, 3, 3, 3, 3, 1, 3, 2, 2, 1, 2, 3, 2, 3, 2, 2, 2, 2, 2, 3, 3, 3, 2, 1, 3, 2, 2, 2, 3, 1
                Tuple2.of(1L,1L),
                Tuple2.of(1L,2L),
                Tuple2.of(1L,2L),
                Tuple2.of(1L,2L),
                Tuple2.of(1L,2L),
                Tuple2.of(1L,1L),
                Tuple2.of(1L,2L),
                Tuple2.of(1L,2L),
                Tuple2.of(1L,3L),
                Tuple2.of(1L,3L),
                Tuple2.of(1L,3L),
                Tuple2.of(1L,3L),
                Tuple2.of(1L,3L),
                Tuple2.of(1L,3L),
                Tuple2.of(1L,3L),
                Tuple2.of(1L,1L),
                Tuple2.of(1L,3L),
                Tuple2.of(1L,2L),
                Tuple2.of(1L,2L),
                Tuple2.of(1L,1L),
                Tuple2.of(1L,2L),
                Tuple2.of(1L,3L),
                Tuple2.of(1L,2L),
                Tuple2.of(1L,3L),
                Tuple2.of(1L,2L),
                Tuple2.of(1L,2L),
                Tuple2.of(1L,2L),
                Tuple2.of(1L,2L),
                Tuple2.of(1L,2L),
                Tuple2.of(1L,3L),
                Tuple2.of(1L,3L),
                Tuple2.of(1L,3L),
                Tuple2.of(1L,2L),
                Tuple2.of(1L,1L),
                Tuple2.of(1L,3L),
                Tuple2.of(1L,2L),
                Tuple2.of(1L,2L),
                Tuple2.of(1L,2L),
                Tuple2.of(1L,3L),
                Tuple2.of(1L,1L)

        );

        inputStream
                .keyBy(0)
                .flatMap(new LoadWithKeyedState())
                .setParallelism(1)
                .print().setParallelism(1);

        env.execute();
    }
}
