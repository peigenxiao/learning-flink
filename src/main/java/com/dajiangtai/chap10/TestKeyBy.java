package com.dajiangtai.chap10;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;

/**
 * @Author: 🐟lifei🐟
 * @Date: 2018/12/18 下午1:53
 */
public class TestKeyBy {
    public static void main(String[] args) throws Exception {
        //统计各班语文成绩最高分是谁
        final StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple4<String,String,String,Integer>> input=env.fromElements(TRANSCRIPT);
//        System.out.println("-----------"+input.getParallelism());
        //input.print();

        KeyedStream<Tuple4<String,String,String,Integer>,Tuple> keyedStream = input.keyBy("f0");


//        KeyedStream<Tuple4<String,String,String,Integer>,String> keyedStream = input.keyBy(new KeySelector<Tuple4<String, String, String, Integer>, String>() {
//
//            @Override
//            public String getKey(Tuple4<String, String, String, Integer> value) throws Exception {
//                return value.f0;
//            }
//        });


//        keyedStream.process(new KeyedProcessFunction<Tuple, Tuple4<String,String,String,Integer>, Object>() {
//
//            @Override
//            public void processElement(Tuple4<String, String, String, Integer> value, Context ctx, Collector<Object> out) throws Exception {
//                System.out.println(ctx.getCurrentKey());
//            }
//        });
        //System.out.println("***********"+keyedStream.getParallelism());

//        System.out.println("---------444444---"+keyedStream.max(3).getParallelism());
        keyedStream.maxBy("f3").print();

        env.execute();

//        SingleOutputStreamOperator<Tuple4<String,String,String,Integer>> sumed=keyed.min(3);
//
//        //使用了DataStreamUtils就不需要env.execute()
//        Iterator<Tuple4<String,String,String,Integer>> it=DataStreamUtils.collect(sumed);
//
//        while (it.hasNext()){
//            System.out.println(it.next());
//        }

    }

    public static final Tuple4[] TRANSCRIPT = new Tuple4[] {
            Tuple4.of("class1","张三","语文",100),
            Tuple4.of("class1","李四","语文",78),
            Tuple4.of("class1","王五","语文",99),
            Tuple4.of("class2","赵六","语文",81),
            Tuple4.of("class2","钱七","语文",59),
            Tuple4.of("class2","马二","语文",97)
    };
}
