package xyz.akumano.kafka.common;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;

import java.util.Map;

// A subclass must have:
// - a no-argument constructor, as avro.reflect needs it.
// - private static avroUtils = new TypeAvroUtils(), which getAvroUtils() refers to.
// - public static class TypeSerializer extends AvroSerializer<T> , and
// - public static class TypeDeserializer extends AvroDeserializer<T> implements Deserializer<T>
// Note on deserializer declaration: Kafka Consumer is happy without the `implements Deserializer` declaration, but Beam isn't.
// The serializer class doesn't have the same issue. Must have something to do with the reflection facility of Beam or type erasure of abstract classes.
abstract public class AvroSerializable {
    // this method should be here, and not in AvroSerializer, because AvroSerializer doesn't know  which avroUtils to use.
    public byte[] serialize() {
        AvroUtils avroUtils = getAvroUtils();
        GenericData.Record avroRecord = new GenericData.Record(avroUtils.schema);
        fieldsMap().forEach((key, val) -> avroRecord.put(key,val));
        return avroUtils.recordInjection.apply(avroRecord);
    };
    abstract protected Map<String, Object> fieldsMap();
    // in a subclass, return a reference to the static field `avroUtils`
    abstract public AvroUtils getAvroUtils();
}

