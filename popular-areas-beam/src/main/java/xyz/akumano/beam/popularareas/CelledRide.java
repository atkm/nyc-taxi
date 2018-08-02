package xyz.akumano.beam.popularareas;

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

import java.util.Map;
import java.util.Objects;

// TODO: could this class inherit RawRide?
@DefaultCoder(AvroCoder.class)
public class CelledRide extends AvroSerializable {
    private final String pickup_datetime;
    private final double pickup_latitude, pickup_longitude;
    private final int pickup_cellid;

    public static AvroUtils avroUtils = new AvroUtils(CelledRide.class);
    @Override
    public AvroUtils getAvroUtils() {
        return avroUtils;
    }

    public CelledRide() {
        pickup_datetime = "";
        pickup_latitude = 0.0;
        pickup_longitude = 0.0;
        pickup_cellid = 0;
    }
    public CelledRide(
            String pickup_datetime,
            double pickup_latitude,
            double pickup_longitude,
            int pickup_cellid
    ) {
        this.pickup_datetime = pickup_datetime;
        this.pickup_latitude = pickup_latitude;
        this.pickup_longitude = pickup_longitude;
        this.pickup_cellid = pickup_cellid;
    }

    public String getPickupDatetime() {
        return pickup_datetime;
    }
    public double getPickupLatitude() {
        return pickup_latitude;
    }
    public double getPickupLongitude() {
        return pickup_longitude;
    }
    public int getPickupCellId() {
        return pickup_cellid;
    }

    @Override
    public Map<String, Object> fieldsMap() {
        return ImmutableMap.of(
                "pickup_datetime", pickup_datetime,
                "pickup_latitude", pickup_latitude,
                "pickup_longitude", pickup_longitude,
                "pickup_cellid", pickup_cellid);
    }

    public static class CelledRideSerializer extends AvroSerializer<CelledRide> {}
    public static class CelledRideDeserializer extends AvroDeserializer<CelledRide> implements Deserializer<CelledRide> {
        protected CelledRide recordToType(GenericRecord record) {
            String pickup_datetime = record.get("pickup_datetime").toString();
            double pickup_latitude = (Double)record.get("pickup_latitude");
            double pickup_longitude = (Double)record.get("pickup_longitude");
            int pickup_cellid = (int) record.get("pickup_cellid");
            return new CelledRide(pickup_datetime, pickup_latitude, pickup_longitude, pickup_cellid);
        }
        protected Injection<GenericRecord, byte[]> getRecordInjection() {
            return CelledRide.avroUtils.recordInjection;
        }
    }

    @Override public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("pickup_datetime", pickup_datetime)
                .add("pickup_latitude", pickup_latitude)
                .add("pickup_longitude", pickup_longitude)
                .add("pickup_cellid", pickup_cellid)
                .toString();
    }

    @Override public boolean equals(Object that) {
        if (this == that) return true;
        if (!(that instanceof CelledRide)) return false;
        CelledRide thatRide = (CelledRide) that;
        return this.pickup_datetime.equals(thatRide.pickup_datetime) &&
                this.pickup_latitude == thatRide.pickup_latitude &&
                this.pickup_longitude == thatRide.pickup_longitude &&
                this.pickup_cellid == thatRide.pickup_cellid;
    }

    @Override
    public int hashCode() {
        return Objects.hash(pickup_datetime, pickup_latitude, pickup_longitude, pickup_cellid);
    }


}
