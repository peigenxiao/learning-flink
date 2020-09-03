package com.dajiangtai.dbus.canal;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.otter.canal.protocol.FlatMessage;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @Author: 🐟lifei🐟
 * @Date: 2019/3/3 下午3:12
 */
public class FlatMessageSchema implements DeserializationSchema<FlatMessage>, SerializationSchema<FlatMessage> {
    @Override
    public FlatMessage deserialize(byte[] message) throws IOException {
        return JSON.parseObject(new String(message), new TypeReference<FlatMessage>() {});
    }

    @Override
    public boolean isEndOfStream(FlatMessage nextElement) {
        return false;
    }

    @Override
    public byte[] serialize(FlatMessage element) {
        return element.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public TypeInformation<FlatMessage> getProducedType() {
        return TypeInformation.of(new TypeHint<FlatMessage>() {});

    }
}
