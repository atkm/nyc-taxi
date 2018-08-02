package xyz.akumano.beam.popularareas;

import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.TimestampPolicy;
import org.apache.beam.sdk.io.kafka.TimestampPolicyFactory;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.kafka.common.TopicPartition;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.Instant;

import java.util.Optional;

// A perfect watermark is used for now. The source needs to be sorted.
// TODO: Read TaxiRideSource from data artisans training to implement a more sophisticated watermark strategy.
// ref: org.apache.beam.sdk.io.kafka.TimestampPolicyFactory.LogAppendTimePolicy
public class PickupTimePolicyFactory extends TimestampPolicyFactory<Integer, CelledRide> {

    @Override
    public TimestampPolicy<Integer, CelledRide> createTimestampPolicy(TopicPartition tp, Optional<Instant> previousWatermark) {
        return new PickupTimePolicy(previousWatermark);
    }

    class PickupTimePolicy extends TimestampPolicy<Integer, CelledRide> {
        //private static final Duration IDLE_WATERMARK_DELTA = Duration.standardSeconds(2);
        protected Instant currentWatermark;

        public PickupTimePolicy(Optional<Instant> previousWatermark) {
            currentWatermark = previousWatermark.orElse(BoundedWindow.TIMESTAMP_MIN_VALUE);
        }

        @Override
        public Instant getTimestampForRecord(TimestampPolicy.PartitionContext ctx, KafkaRecord<Integer, CelledRide> record) {
            CelledRide ride = record.getKV().getValue();
            Instant datetime = Instant.parse(ride.getPickupDatetime(), DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"));
            if (datetime.isAfter(currentWatermark))
                currentWatermark = datetime;
            return datetime;
        }

        @Override
        public Instant getWatermark(TimestampPolicy.PartitionContext ctx) {
            return currentWatermark;
        }
    }

}
