package com.dajiangtai.userPurchaseBehaviorTracker.simulator;

import com.cloudwise.sdg.dic.DicInitializer;
import com.cloudwise.sdg.template.TemplateAnalyzer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

/**
 * @Author: 🐟lifei🐟
 * @Date: 2019/1/27 下午9:34
 */
public class ConfigSimulator {
    /**
     * {"channel":"APP","registerDate":"2018-01-01","historyPurchaseTimes":0,"maxPurchasePathLength":3}
     */
    public static void main(String[] args) throws Exception{

        String config="{\"channel\":\"APP\",\"registerDate\":\"2018-01-01\",\"historyPurchaseTimes\":0,\"maxPurchasePathLength\":3}";

        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.216.130:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer<String, String>(props);
        ProducerRecord record;
        record = new ProducerRecord<String, String>(
                "purchasePathAnalysisConf",
                null,
                new Random().nextInt()+"",
                config);
        producer.send(record);
        producer.close();

    }
}
