package xyz.akumano.kafka.common;

import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class AvroSerializer<T extends AvroSerializable> implements Serializer<T> {

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            // nothing to do
        }

        @Override
        public byte[] serialize(String topic, T data) {
            return data.serialize();
        }

        @Override
        public void close() {
            // nothing to do
        }
}
