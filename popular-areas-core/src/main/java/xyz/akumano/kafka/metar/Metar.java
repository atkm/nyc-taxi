package xyz.akumano.kafka.metar;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.twitter.bijection.Injection;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import xyz.akumano.kafka.common.AvroDeserializer;
import xyz.akumano.kafka.common.AvroSerializable;
import xyz.akumano.kafka.common.AvroSerializer;
import xyz.akumano.kafka.common.AvroUtils;

import java.util.Map;
import java.util.Objects;

@DefaultCoder(AvroCoder.class)
public class Metar extends AvroSerializable {
    private final String datetime;
    private final double fahrenheit, precip_in; // precipitation in inches

    public static AvroUtils avroUtils = new AvroUtils(Metar.class);
    @Override
    public AvroUtils getAvroUtils() { return avroUtils; }

    public Metar() {
        datetime = "";
        fahrenheit = 0.0;
        precip_in = 0.0;
    }
    public Metar(
            String datetime,
            double fahrenheit,
            double precip_in
    ) {
        this.datetime = datetime;
        this.fahrenheit = fahrenheit;
        this.precip_in = precip_in;
    }

    public String getDatetime() { return datetime; }
    public double getFahrenheit() { return fahrenheit; }
    public double getPrecipIn() { return precip_in; }

    @Override
    public Map<String, Object> fieldsMap() {
        return ImmutableMap.of(
                "datetime", datetime,
                "fahrenheit", fahrenheit,
                "precip_in", precip_in
        );
    }
    public static class MetarSerializer extends AvroSerializer<Metar> {}
    public static class MetarDeserializer extends AvroDeserializer<Metar> implements Deserializer<Metar> {
        protected Metar recordToType(GenericRecord record) {
            String datetime = record.get("datetime").toString();
            double fahrenheit = (Double)record.get("fahrenheit");
            double precip_in = (Double)record.get("precip_in");
            return new Metar(datetime, fahrenheit, precip_in);
        }
        protected Injection<GenericRecord, byte[]> getRecordInjection() {
            return Metar.avroUtils.recordInjection;
        }
    }
    public static Serde<Metar> MetarSerde = Serdes.serdeFrom(new MetarSerializer(), new MetarDeserializer());

    @Override public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("datetime", datetime)
                .add("fahrenheit", fahrenheit)
                .add("precip_in", precip_in)
                .toString();
    }
    @Override public boolean equals(Object that) {
        if (this == that) return true;
        if (!(that instanceof Metar)) return false;
        Metar thatRide = (Metar) that;
        return this.datetime.equals(thatRide.datetime) &&
                this.fahrenheit == thatRide.fahrenheit &&
                this.precip_in == thatRide.precip_in;
    }
    @Override
    public int hashCode() {
        return Objects.hash(datetime, fahrenheit, precip_in);
    }
}
