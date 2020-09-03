package com.dajiangtai.chap13;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.util.Collector;

/**
 * @Author: 🐟lifei🐟
 * @Date: 2019/1/13 下午2:11
 */
public class TestCoGroup {
    public static void main(String[] args) throws Exception {
        /**
         * 1、创建一个socket stream。本机9000端口。输入的字符串以空格为界分割成Array[String]。然后再取出其中前两个元素组成(String, String)类型的tuple。
         * 2、join条件为两个流中的数据((String, String)类型)第一个元素相同。
         * 3、为测试方便，这里使用session window。只有两个元素到来时间前后相差不大于30秒之时才会被匹配。
         * Session window的特点为，没有固定的开始和结束时间，只要两个元素之间的时间间隔不大于设定值，就会分配到同一个window中，否则后来的元素会进入新的window）。
         * 4、将window默认的trigger修改为count trigger。这里的含义为每到来一个元素，都会立刻触发计算。
         * 5、无论能不能匹配，2边的数据都被coGroup到一起了
         */
        final StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, String>> stream1=env
                .socketTextStream("127.0.0.1",9000)
                .map(new MapFunction<String, Tuple2<String, String>>() {

                    @Override
                    public Tuple2<String, String> map(String value) throws Exception {
                        String[] arr=value.split(" ");
                        return Tuple2.of(arr[0],arr[1]);
                    }
                });

        DataStream<Tuple2<String, String>> stream2=env
                .socketTextStream("127.0.0.1",9001)
                .map(new MapFunction<String, Tuple2<String, String>>() {

                    @Override
                    public Tuple2<String, String> map(String value) throws Exception {
                        String[] arr=value.split(" ");
                        return Tuple2.of(arr[0],arr[1]);
                    }
                });

        stream1.coGroup(stream2)
                .where(new KeySelector<Tuple2<String,String>, String>() {

                    @Override
                    public String getKey(Tuple2<String, String> value) throws Exception {
                        return value.f0;
                    }
                }).equalTo(new KeySelector<Tuple2<String,String>, String>() {

                    @Override
                    public String getKey(Tuple2<String, String> value) throws Exception {
                        return value.f0;
                    }
                })
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(30)))
                .trigger(CountTrigger.of(1))
                .apply(new CoGroupFunction<Tuple2<String,String>, Tuple2<String,String>, String>() {

                    @Override
                    public void coGroup(Iterable<Tuple2<String, String>> first, Iterable<Tuple2<String, String>> second, Collector<String> out) throws Exception {
                        StringBuilder stringBuilder = new StringBuilder("Data in stream1: \n");
                        for (Tuple2<String, String> item:first) {
                            stringBuilder.append(item.f0 + "<=>" + item.f1 + "\n");
                        }
                        stringBuilder.append("Data in stream2: \n");
                        for (Tuple2<String, String> item:second) {
                            stringBuilder.append(item.f0 + "<=>" + item.f1 + "\n");
                        }
                        out.collect(stringBuilder.toString());
                    }
                }).print();

        env.execute();
    }
}
