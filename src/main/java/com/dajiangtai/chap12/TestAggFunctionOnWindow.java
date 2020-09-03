package com.dajiangtai.chap12;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: 🐟lifei🐟
 * @Date: 2019/1/13 上午10:37
 */
public class TestAggFunctionOnWindow {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple3<String,String,Long>> input=env.fromElements(ENGLISH_TRANSCRIPT);

        //求各班级英语平均分
        DataStream<Double> avgScore=input.
                keyBy(0).
                countWindow(2).
                aggregate(new AverageAggregate());
        avgScore.print();

        env.execute();
    }

    /**
     * 英语成绩
     */
    public static final Tuple3[] ENGLISH_TRANSCRIPT = new Tuple3[] {
            Tuple3.of("class1","张三",100L),
            Tuple3.of("class1","李四",78L),
            Tuple3.of("class1","王五",99L),
            Tuple3.of("class2","赵六",81L),
            Tuple3.of("class2","钱七",59L),
            Tuple3.of("class2","马二",97L)
    };

    /**
     * 求第二个字段的平均值(sum/count)
     */
    private static class AverageAggregate
            implements AggregateFunction<Tuple3<String, String,Long>, Tuple2<Long, Long>, Double> {
        /**
         * 创建累加器来保存中间状态(sum和count)
         * @return
         */
        @Override
        public Tuple2<Long, Long> createAccumulator() {
            return new Tuple2<>(0L, 0L);
        }

        /**
         * 将元素添加到累加器并返回新的累加器value
         * @param value
         * @param accumulator
         * @return
         */
        @Override
        public Tuple2<Long, Long> add(Tuple3<String, String, Long> value, Tuple2<Long, Long> accumulator) {
            //来一个元素计算一下sum和count并保存中间结果到累加器
            return new Tuple2<>(accumulator.f0 + value.f2, accumulator.f1 + 1L);
        }

        /**
         * 从累加器提取结果
         * @param accumulator
         * @return
         */
        @Override
        public Double getResult(Tuple2<Long, Long> accumulator) {
            return ((double) accumulator.f0) / accumulator.f1;
        }

        @Override
        public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
            return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
        }

    }
}
