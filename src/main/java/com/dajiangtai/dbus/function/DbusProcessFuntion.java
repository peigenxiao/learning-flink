package com.dajiangtai.dbus.function;

import com.alibaba.otter.canal.protocol.FlatMessage;
import com.dajiangtai.dbus.enums.FlowStatusEnum;
import com.dajiangtai.dbus.incrementsync.IncrementSyncApp;
import com.dajiangtai.dbus.model.Flow;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;


/**
 * @Author: 🐟lifei🐟
 * @Date: 2019/3/10 下午7:06
 * 连接FlatMessage和对应付的Flow配置
 */
public class DbusProcessFuntion extends KeyedBroadcastProcessFunction<String,FlatMessage,Flow,Tuple2<FlatMessage,Flow>> {
    @Override
    public void processElement(FlatMessage value, ReadOnlyContext ctx, Collector<Tuple2<FlatMessage, Flow>> out) throws Exception {

        Flow flow = ctx.getBroadcastState(IncrementSyncApp.flowStateDescriptor).get(value.getDatabase()+ value.getTable());

        if(null!=flow && flow.getStatus()==FlowStatusEnum.FLOWSTATUS_RUNNING.getCode()){
            out.collect(Tuple2.of(value,flow));
        }
    }

    @Override
    public void processBroadcastElement(Flow flow, Context ctx, Collector<Tuple2<FlatMessage, Flow>> out) throws Exception {
        BroadcastState<String, Flow> state = ctx.getBroadcastState(IncrementSyncApp.flowStateDescriptor);
        state.put(flow.getDatabaseName()+flow.getTableName(), flow);
    }
}
