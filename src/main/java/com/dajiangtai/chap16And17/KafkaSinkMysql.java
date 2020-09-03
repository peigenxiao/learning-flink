package com.dajiangtai.chap16And17;

/**
 * Created by Administrator on 2020/7/4.
 */
import com.google.gson.Gson;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class KafkaSinkMysql {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        props.put("bootstrap.servers", "peigen004:9092");
        props.put("group.id", "test");
        //props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //props.put("enable.auto.commit", "true");
        props.put("auto.offset.reset", "latest");
        SingleOutputStreamOperator<Student> empStream = env.addSource(new FlinkKafkaConsumer011<String>(
                "student",   //这个 kafka topic 需和生产消息的 topic 一致
                new SimpleStringSchema(),
                props)).setParallelism(1)
                .map(new MapFunction<String, Student>() {
                    @Override
                    public Student map(String string) throws Exception {
                        Gson gson = new Gson();
                        return gson.fromJson(string,Student.class);
                    }
                }); //，解析字符串成JSON对象

        //开个一分钟的窗口去聚合
        empStream.timeWindowAll(Time.minutes(1)).apply(new AllWindowFunction<Student, List<Student>, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<Student> values, Collector<List<Student>> out) throws Exception {
                ArrayList<Student> employees = Lists.newArrayList(values);
                if (employees.size() > 0) {
                    System.out.println("1 分钟内收集到 employee 的数据条数是：" + employees.size());
                    out.collect(employees);
                }
            }
        });
             //   .addSink(new SinkToMySQL());

        //empStream.print(); //调度输出
        env.execute("flink kafka to Mysql");

    }
}
