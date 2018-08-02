package xyz.akumano.beam.popularareas;

import avro.shaded.com.google.common.collect.ImmutableMap;
import com.google.common.base.MoreObjects;
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

/**
 * The datetime fields contains the timestamp at the end of a window from which this count is produced.
 */
@DefaultCoder(AvroCoder.class)
public class CellCount extends AvroSerializable {

    private final String datetime;
    private final int cell_id;
    private final long count;

    public String getDatetime() {
        return datetime;
    }
    public int getCellId() {
        return cell_id;
    }
    public long getCount() {
        return count;
    }

    public static AvroUtils avroUtils = new AvroUtils(CellCount.class);
    @Override
    public AvroUtils getAvroUtils() {
        return avroUtils;
    }

    // TODO: a better representation of time? Avro's logical type timestamp-millis
    public CellCount() {
        datetime = "";
        cell_id = 0;
        count = 0;
    }
    public CellCount(
            String datetime,
            int cell_id,
            long count
    ) {
        this.datetime = datetime;
        this.cell_id = cell_id;
        this.count = count;
    }

    @Override
    public Map<String, Object> fieldsMap() {
        return ImmutableMap.of(
        "datetime", datetime,
        "cell_id", cell_id,
        "count", count);
    }

    public static class CellCountSerializer extends AvroSerializer<CellCount> {}
    public static class CellCountDeserializer extends AvroDeserializer<CellCount> implements Deserializer<CellCount> {
        protected CellCount recordToType(GenericRecord record) {
            String datetime = record.get("datetime").toString();
            int cell_id = (int) record.get("cell_id");
            long count = (long) record.get("count");
            return new CellCount(datetime, cell_id, count);
        }
        protected Injection<GenericRecord, byte[]> getRecordInjection() {
            return CellCount.avroUtils.recordInjection;
        }
    }
    public static Serde<CellCount> CellCountSerde = Serdes.serdeFrom(new CellCountSerializer(), new CellCountDeserializer());


    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("datetime", datetime)
                .add("cell_id", cell_id)
                .add("count", count)
                .toString();
    }
    @Override
    public boolean equals(Object that) {
        if (this == that) return true;
        if (!(that instanceof CellCount)) return false;
        CellCount thatRide = (CellCount) that;
        return this.datetime.equals(thatRide.datetime) &&
                this.cell_id == thatRide.cell_id
                && this.count == thatRide.count;
    }
    @Override
    public int hashCode() {
        return Objects.hash(datetime, cell_id, count);
    }


}
