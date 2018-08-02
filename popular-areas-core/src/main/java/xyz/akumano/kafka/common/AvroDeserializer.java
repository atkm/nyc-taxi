package xyz.akumano.kafka.common;

import com.twitter.bijection.Injection;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

abstract public class AvroDeserializer<T extends AvroSerializable> implements Deserializer<T> {

    abstract protected Injection<GenericRecord, byte[]> getRecordInjection();
    abstract protected T recordToType(GenericRecord record);

    public void configure(Map<String, ?> configs, boolean isKey) {
        // nothing to do
    }
    public T deserialize(String topic, byte[] bytes) {
        Injection<GenericRecord, byte[]> recordInjection = getRecordInjection();
        GenericRecord record = recordInjection.invert(bytes).get();
        return recordToType(record);
    }

    public void close() {
        // nothing to do
    }
}
