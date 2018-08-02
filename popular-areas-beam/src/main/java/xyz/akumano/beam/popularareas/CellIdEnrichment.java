package xyz.akumano.beam.popularareas;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import xyz.akumano.kafka.rawride.RawRide;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

// The result is written to a kafka topic instead of piping it to the popular-areas function, because there are other
// services that want this information (e.g. an ETL job that moves CelledRides to HDFS).
public class CellIdEnrichment {

    /**
     * Given a RawRide instance, computes the cell that a lat-lon pair belongs to,
     * and appends the cell id to the instance to create a CelledRide instance.
     */
    public static class CellIdFn extends DoFn<RawRide, KV<Integer, CelledRide>> {
        @ProcessElement
        public void processElement(ProcessContext ctx) throws Exception {
            RawRide ride = ctx.element();
            String datetime = ride.getPickupDatetime();
            double lat = ride.getPickupLatitude(), lon = ride.getPickupLongitude();
            try {
                int cellId = GeoUtils.toGridCell(new GeoUtils.Coord(lat, lon));
                CelledRide enrichedRide = new CelledRide(datetime, lat, lon, cellId);
                KV<Integer, CelledRide> keyedRide = KV.of(cellId, enrichedRide);
                ctx.output(keyedRide);
            } catch (Exception e) {
                System.err.printf("Conversion to cell id failed when (lat,lon)=(%f,%f)%n", lat, lon);
                System.err.println(e.getMessage());
            }
        }
    }

    public static class RawRideReader extends PTransform<PBegin, PCollection<RawRide>> {
        private final String bootstrapServer, sourceTopicName;
        private final Map<String, Object> consumerConfig;
        public RawRideReader(String sourceTopicName,
                         String bootstrapServer,
                         Map<String, Object> consumerConfig) {
            this.sourceTopicName = sourceTopicName;
            this.bootstrapServer = bootstrapServer;
            this.consumerConfig = consumerConfig;
        }

        @Override
        public PCollection<RawRide> expand(PBegin pBegin) {
            return pBegin.apply(KafkaIO.<String, RawRide>read()
                    .withBootstrapServers(bootstrapServer)
                    .withTopic(sourceTopicName)
                    .withKeyDeserializer(StringDeserializer.class)
                    .withValueDeserializer(RawRide.RawRideDeserializer.class)
                    .updateConsumerProperties(consumerConfig)
                    .withoutMetadata()
            )
                    .apply(Values.<RawRide>create());
        }
    }

    public static class EnrichmentTransform extends PTransform<PBegin, PDone> {
        private final String sourceTopicName, sinkTopicName, bootstrapServer;
        private final Map<String, Object> consumerConfig;

        public EnrichmentTransform(String sourceTopicName,
                                  String sinkTopicName,
                                  String bootstrapServer,
                                  Map<String, Object> consumerConfig) {
            this.sourceTopicName = sourceTopicName;
            this.sinkTopicName = sinkTopicName;
            this.bootstrapServer = bootstrapServer;
            this.consumerConfig = consumerConfig;
        }

        @Override
        public PDone expand(PBegin pBegin) {
            PCollection<RawRide> rides = pBegin.apply(new RawRideReader(sourceTopicName, bootstrapServer, consumerConfig));
            PCollection<KV<Integer, CelledRide>> enrichedRides = rides.apply(ParDo.of(new CellIdFn()));
            return enrichedRides.apply(KafkaIO.<Integer, CelledRide>write()
                    .withBootstrapServers(bootstrapServer)
                    .withKeySerializer(IntegerSerializer.class)
                    .withValueSerializer(CelledRide.CelledRideSerializer.class)
                    .withTopic(sinkTopicName)
            );
        }
    }

    public static void main(String args[]) {

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        String sourceTopicName = "raw-rides-2014-05-small";
        //String sourceTopicName = "raw-rides-2014-05-tiny";
        String sinkTopicName = "enriched-rides-2014-05-small";
        //String sinkTopicName = "enriched-rides-2014-05-tiny";
        String bootstrapServer = "localhost:9092";

        Map<String, Object> consumerConfig = new HashMap<>();
        consumerConfig.put("auto.offset.reset", "earliest");

        // clear the sink topic
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        AdminClient admin = AdminClient.create(props);
        admin.deleteTopics(Collections.singletonList(sinkTopicName));

        pipeline.apply(new EnrichmentTransform(
                sourceTopicName, sinkTopicName, bootstrapServer, consumerConfig
        ));

        pipeline.run().waitUntilFinish();
    }
}
