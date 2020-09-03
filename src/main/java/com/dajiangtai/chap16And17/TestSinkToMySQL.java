package com.dajiangtai.chap16And17;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class TestSinkToMySQL {
    public static final String TOPIC = "student";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.216.130:9092");
       // props.put("zookeeper.connect", "192.168.216.130:2181");
        props.put("group.id", "group1");
      //  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
      //  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");

        FlinkKafkaConsumer011<String> myConsumer=new FlinkKafkaConsumer011<>(TOPIC, new SimpleStringSchema(), props);

        SingleOutputStreamOperator<Student> student = env.addSource(myConsumer).setParallelism(1)
                .map(string -> JSON.parseObject(string, Student.class)); //Fastjson 解析字符串成 student 对象

        student.addSink(new SinkToMySQL()); //数据 sink 到 mysql

        env.execute("Flink add sink");
    }
}