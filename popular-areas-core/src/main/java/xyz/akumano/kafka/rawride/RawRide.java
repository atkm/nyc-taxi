package xyz.akumano.kafka.rawride;

import avro.shaded.com.google.common.collect.ImmutableMap;
import com.google.common.base.MoreObjects;
import com.twitter.bijection.Injection;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.kafka.common.serialization.Deserializer;
import xyz.akumano.kafka.common.AvroDeserializer;
import xyz.akumano.kafka.common.AvroSerializable;
import xyz.akumano.kafka.common.AvroSerializer;
import xyz.akumano.kafka.common.AvroUtils;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Objects;

@DefaultCoder(AvroCoder.class)
public class RawRide extends AvroSerializable {
    private final double pickup_latitude, pickup_longitude;
    private final String pickup_datetime;

    public static AvroUtils avroUtils = new AvroUtils(RawRide.class);
    @Override
    public AvroUtils getAvroUtils() {
        return avroUtils;
    }

    public RawRide() {
        pickup_datetime = "";
        pickup_latitude = 0.0;
        pickup_longitude = 0.0;
    }
    public RawRide(
            String pickup_datetime,
            double pickup_latitude,
            double pickup_longitude
    ) {
        this.pickup_datetime = pickup_datetime;
        this.pickup_latitude = pickup_latitude;
        this.pickup_longitude = pickup_longitude;
    }

    public double getPickupLatitude() {
      return pickup_latitude;
    }
    public double getPickupLongitude() {
      return pickup_longitude;
    }
    public String getPickupDatetime() {
      return pickup_datetime;
    }

    // this may be difficult to factor out, since a class has fields that shouldn't be exported.
    // One could export fields that are private and final, but that is likely to result in surprises in the future, when this class is extended.
    // One could export fields that are declared in a constructor, but, again, that sounds like a recipe for an unmaintainable code.
    // One could define String[] fields = {"pickup_datetime", ... } and use Class.getField(fields[0]) to obtain fields of interest.
    //  This approach uses reflection. Serialization is done frequently, so there is an argument for hard-coding that it improves performance.
    @Override
    public Map<String, Object> fieldsMap() {
        return ImmutableMap.of(
        "pickup_datetime", pickup_datetime,
        "pickup_latitude", pickup_latitude,
        "pickup_longitude", pickup_longitude
        );
    }

    public static class RawRideSerializer extends AvroSerializer<RawRide> {}
    public static class RawRideDeserializer extends AvroDeserializer<RawRide> implements Deserializer<RawRide> {
        protected RawRide recordToType(GenericRecord record) {
            String pickup_datetime = record.get("pickup_datetime").toString();
            double pickup_latitude = (Double)record.get("pickup_latitude");
            double pickup_longitude = (Double)record.get("pickup_longitude");
            return new RawRide(pickup_datetime, pickup_latitude, pickup_longitude);
        }
        protected Injection<GenericRecord, byte[]> getRecordInjection() {
            return RawRide.avroUtils.recordInjection;
        }
    }

    @Override public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("pickup_datetime", pickup_datetime)
                .add("pickup_latitude", pickup_latitude)
                .add("pickup_longitude", pickup_longitude)
                .toString();
    }

    // TODO: implement a better double comparison.
    @Override
    public boolean equals(Object that) {
        if (that == null) return false;
        else if (!(that instanceof RawRide)) {
            return false;
        }
        else {
            RawRide thatRide = (RawRide) that;
            return (thatRide.pickup_datetime.equals(pickup_datetime)
                    && thatRide.pickup_latitude == pickup_latitude
                    && thatRide.pickup_longitude == pickup_longitude);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(pickup_datetime, pickup_latitude, pickup_longitude);
    }


    public static void main(String[] args) throws Exception {
        RawRide ride = new RawRide();
        System.out.println(ride.getPickupLatitude() + " of " + ride.toString());
        Field f = RawRide.class.getField("pickup_latitude");
        System.out.println(f.get(ride));
        //for (Constructor c : RawRide.class.getConstructors()) {
        //    System.out.println(c);
        //}
        //for (Field f : RawRide.class.getDeclaredFields()) {
        //    System.out.println(f);
        //}
    }
}
