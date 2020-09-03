package com.dajiangtai.userPurchaseBehaviorTracker;

import com.dajiangtai.userPurchaseBehaviorTracker.function.ConnectedBroadcastProcessFuntion;
import com.dajiangtai.userPurchaseBehaviorTracker.model.Config;
import com.dajiangtai.userPurchaseBehaviorTracker.model.EvaluatedResult;
import com.dajiangtai.userPurchaseBehaviorTracker.model.UserEvent;
import com.dajiangtai.userPurchaseBehaviorTracker.schema.ConfigDeserializationSchema;
import com.dajiangtai.userPurchaseBehaviorTracker.schema.EvaluatedResultSerializationSchema;
import com.dajiangtai.userPurchaseBehaviorTracker.schema.UserEventDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @Author: 🐟lifei🐟
 * @Date: 2019/1/27 下午8:25
 */
@Slf4j
public class Launcher {
    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    public static final String GROUP_ID = "group.id";
    public static final String RETRIES = "retries";
    public static final String INPUT_EVENT_TOPIC = "input-event-topic";
    public static final String INPUT_CONFIG_TOPIC = "input-config-topic";
    public static final String OUTPUT_TOPIC = "output-topic";
    public static final MapStateDescriptor<String, Config> configStateDescriptor =
            new MapStateDescriptor<>(
                    "configBroadcastState",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    TypeInformation.of(new TypeHint<Config>() {}));


    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Checking input parameters
        final ParameterTool params = parameterCheck(args);
        env.getConfig().setGlobalJobParameters(params);
        //setGlobalJobParameters之后，可以在flink的页面看到配置信息
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /**
         * checkpoint
         */
        env.enableCheckpointing(60000L);
        CheckpointConfig checkpointConf=env.getCheckpointConfig();
        checkpointConf.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConf.setMinPauseBetweenCheckpoints(30000L);
        checkpointConf.setCheckpointTimeout(10000L);
        checkpointConf.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        /**
         * StateBackend
         */
//        env.setStateBackend(new FsStateBackend(
//                "hdfs://namenode01.td.com/flink-checkpoints/customer-purchase-behavior-tracker"));

        /**
         * restart策略
         */
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                10, // number of restart attempts
                org.apache.flink.api.common.time.Time.of(30, TimeUnit.SECONDS) // delay
        ));

        /* Kafka consumer */
        Properties consumerProps=new Properties();
        consumerProps.setProperty(BOOTSTRAP_SERVERS, params.get(BOOTSTRAP_SERVERS));
        consumerProps.setProperty(GROUP_ID, params.get(GROUP_ID));

        // 事件流
        final FlinkKafkaConsumer010 kafkaUserEventSource = new FlinkKafkaConsumer010<UserEvent>(
                params.get(INPUT_EVENT_TOPIC),
                new UserEventDeserializationSchema(),consumerProps);

        // (userEvent, userId)
        KeyedStream<UserEvent, String> customerUserEventStream = env
                .addSource(kafkaUserEventSource)
                .assignTimestampsAndWatermarks(new CustomWatermarkExtractor(Time.hours(24)))
                .keyBy(new KeySelector<UserEvent, String>() {
                    @Override
                    public String getKey(UserEvent userEvent) throws Exception {
                        return userEvent.getUserId();
                    }
                });
        //customerUserEventStream.print();

        //配置流
        final FlinkKafkaConsumer010 kafkaConfigEventSource = new FlinkKafkaConsumer010<Config>(
                params.get(INPUT_CONFIG_TOPIC),
                new ConfigDeserializationSchema(), consumerProps);

        final BroadcastStream<Config> configBroadcastStream = env
                .addSource(kafkaConfigEventSource)
                .broadcast(configStateDescriptor);

        //连接两个流
        /* Kafka consumer */
        Properties producerProps=new Properties();
        producerProps.setProperty(BOOTSTRAP_SERVERS, params.get(BOOTSTRAP_SERVERS));
        producerProps.setProperty(RETRIES, "3");

        final FlinkKafkaProducer010 kafkaProducer = new FlinkKafkaProducer010<EvaluatedResult>(
                params.get(OUTPUT_TOPIC),
                new EvaluatedResultSerializationSchema(),
                producerProps);

        /* at_ least_once 设置 */
        kafkaProducer.setLogFailuresOnly(false);
        kafkaProducer.setFlushOnCheckpoint(true);

        DataStream<EvaluatedResult> connectedStream = customerUserEventStream
                .connect(configBroadcastStream)
                .process(new ConnectedBroadcastProcessFuntion());

        connectedStream.addSink(kafkaProducer);

        env.execute("UserPurchaseBehaviorTracker");

    }

    /**
     * 参数校验
     * @param args
     * @return
     */
    public static ParameterTool parameterCheck(String[] args){

        //--bootstrap.servers slave03:9092 --group.id test --input-event-topic purchasePathAnalysisInPut --input-config-topic purchasePathAnalysisConf --output-topic purchasePathAnalysisOutPut

        ParameterTool params=ParameterTool.fromArgs(args);

        params.getProperties().list(System.out);

        if(!params.has(BOOTSTRAP_SERVERS)){
            System.err.println("----------------parameter[bootstrap.servers] is required----------------");
            System.exit(-1);
        }
        if(!params.has(GROUP_ID)){
            System.err.println("----------------parameter[group.id] is required----------------");
            System.exit(-1);
        }
        if(!params.has(INPUT_EVENT_TOPIC)){
            System.err.println("----------------parameter[input-event-topic] is required----------------");
            System.exit(-1);
        }
        if(!params.has(INPUT_CONFIG_TOPIC)){
            System.err.println("----------------parameter[input-config-topic] is required----------------");
            System.exit(-1);
        }
        if(!params.has(OUTPUT_TOPIC)){
            System.err.println("----------------parameter[output-topic] is required----------------");
            System.exit(-1);
        }

        return params;
    }

    private static class CustomWatermarkExtractor extends BoundedOutOfOrdernessTimestampExtractor<UserEvent> {

        public CustomWatermarkExtractor(Time maxOutOfOrderness) {
            super(maxOutOfOrderness);
        }
        @Override
        public long extractTimestamp(UserEvent element) {
            return element.getEventTime();
        }

    }
}
