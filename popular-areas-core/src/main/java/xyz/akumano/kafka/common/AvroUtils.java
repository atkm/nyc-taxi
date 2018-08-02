package xyz.akumano.kafka.common;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;

// the class contains the schema and recordInjection for a given class.
public class AvroUtils {

    public final Schema schema;
    public final Injection<GenericRecord, byte[]> recordInjection;

    public AvroUtils(Class k) {
        this.schema  = ReflectData.get().getSchema(k);
        this.recordInjection = GenericAvroCodecs.toBinary(schema);
    }
}
