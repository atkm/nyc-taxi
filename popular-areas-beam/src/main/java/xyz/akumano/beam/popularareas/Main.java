package xyz.akumano.beam.popularareas;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.TimestampPolicy;
import org.apache.beam.sdk.io.kafka.TimestampPolicyFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.json.JSONObject;
import xyz.akumano.kafka.rawride.RawRide;

import static xyz.akumano.beam.popularareas.CellIdEnrichment.*;
import static xyz.akumano.beam.popularareas.PopularCells.*;

import java.util.*;


// combine CellIdEnrichment and PopularCells -- do not write to Kafka after CellIdEnrichment
// emit (datetime, lat-lon pair of the center of the cell, count) to ElasticSearch
public class Main {

    // Parses CellCount to the JSON format that ElasticSearch wants: {String datetime, String "(lat,lon)", long count}
    private static class PopularCellsToJson extends DoFn<CellCount, String> {
        @ProcessElement
        public void processElement(ProcessContext ctx) {
            CellCount cellCount = ctx.element();
            int cellId = cellCount.getCellId();
            GeoUtils.Coord pair = GeoUtils.gridCellCenter(cellId);
            String jsonRecord = new JSONObject()
                    .put("time", cellCount.getDatetime())
                    .put("location", pair.getLat() + "," + pair.getLon())
                    .put("cnt", cellCount.getCount())
                    .toString();

            ctx.output(jsonRecord);
        }
    }

    // TODO: use the TimestampedRecord to merge this class with PickupTimePolicyFactory.java.
    static class PickupTimePolicyFactory extends TimestampPolicyFactory<Integer, RawRide> {

        @Override
        public TimestampPolicy<Integer, RawRide> createTimestampPolicy(TopicPartition tp, Optional<Instant> previousWatermark) {
            return new PickupTimePolicy(previousWatermark);
        }

        class PickupTimePolicy extends TimestampPolicy<Integer, RawRide> {
            //private static final Duration IDLE_WATERMARK_DELTA = Duration.standardSeconds(2);
            protected Instant currentWatermark;

            public PickupTimePolicy(Optional<Instant> previousWatermark) {
                currentWatermark = previousWatermark.orElse(BoundedWindow.TIMESTAMP_MIN_VALUE);
            }

            @Override
            public Instant getTimestampForRecord(TimestampPolicy.PartitionContext ctx, KafkaRecord<Integer, RawRide> record) {
                RawRide ride = record.getKV().getValue();
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

    public static class PopularCellsEndToEnd extends PTransform<PBegin, PDone> {
        private final String sourceTopicName, bootstrapServer, esHost, esIndex, esType;
        private final Map<String, Object> consumerConfig;
        private final int windowSize, windowFrequency, popularityThreshold;

        public PopularCellsEndToEnd(String sourceTopicName,
                                    String bootstrapServer,
                                    Map<String, Object> consumerConfig,
                                    String esHost,
                                    String esIndex,
                                    String esType,
                                    int windowSize,
                                    int windowFrequency,
                                    int popularityThreshold) {
            this.sourceTopicName = sourceTopicName;
            this.bootstrapServer = bootstrapServer;
            this.consumerConfig = consumerConfig;
            this.esHost = esHost;
            this.esIndex = esIndex;
            this.esType = esType;
            this.windowSize = windowSize;
            this.windowFrequency = windowFrequency;
            this.popularityThreshold = popularityThreshold;
        }

        @Override
        public PDone expand(PBegin pBegin) {
            PCollection<RawRide> rawRides = pBegin.apply(
                    new KafkaUtils.TopicReader<String, RawRide>(sourceTopicName, bootstrapServer,
                            consumerConfig, StringDeserializer.class, RawRide.RawRideDeserializer.class,
                            new PickupTimePolicyFactory())
            )
                    .apply(Values.<RawRide>create());
            PCollection<KV<Integer, CelledRide>> enrichedRides = rawRides.apply(ParDo.of(new CellIdFn()));
            PCollection<KV<Integer, CellCount>> rideCounts = enrichedRides
                    .apply(Keys.<Integer>create())
                    .apply(new CountByCellId(windowSize, windowFrequency));

            PCollection<CellCount> popularCells = rideCounts.apply(ParDo.of(new FilterPopularCells(popularityThreshold)))
                    .apply(Values.<CellCount>create());

            PCollection<String> popularCellsJSON = popularCells.apply(ParDo.of(new PopularCellsToJson()));
            String[] hosts = {esHost};
            return popularCellsJSON.apply(ElasticsearchIO.write().withConnectionConfiguration(
                    ElasticsearchIO.ConnectionConfiguration.create(
                            hosts, esIndex, esType)
            ));

            //return popularCells.apply(new KafkaUtils.TopicWriter<Integer, CellCount>(
            //        sinkTopicName, bootstrapServer, IntegerSerializer.class, CellCount.CellCountSerializer.class
            //));

        }
    }

    /**
     * Ref: WordCount from the official quickstart.
     * @param args kafka-bootstrap-server,
     *             sourceTopicName,
     *             sinkTopicName,
     */
    public static void main(String[] args) {
        //PipelineOptions options = PipelineOptionsFactory.create();
        // provide options with -Dexec.args. Isn't working.
        //PipelineOptions options = PipelineOptionsFactory
        //        .fromArgs(args).withValidation().create();
        
        // provide options programatically. Working.
        String bucket = "gs://bucket-name/";
        String project = "project-name";
        DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
        options.setProject(project);
        options.setRegion("us-west1");
        options.setStagingLocation(bucket + "staging/");
        options.setGcpTempLocation(bucket + "dataflow-tmp/");
        options.setRunner(DataflowRunner.class);
        Pipeline pipeline = Pipeline.create(options);

        String sourceTopicName = "raw-rides";
        String bootstrapServer = "10.138.0.2:9092"; // kafka-1
        //String bootstrapServer = "localhost:9092";

        Map<String, Object> consumerConfig = new HashMap<>();
        consumerConfig.put("auto.offset.reset", "earliest");

        //String esHost = args[0],
        String esHost = "http://10.138.0.8:9200", // elasticsearch-5-6-10--2
                esIndex = "nyc-taxi",
                esMapping = "popular-places";

        int popularityThreshold = 20;
        pipeline.apply(new PopularCellsEndToEnd(sourceTopicName, bootstrapServer, consumerConfig,
                esHost, esIndex, esMapping,
                15, 5, popularityThreshold));
        pipeline.run().waitUntilFinish();
    }
}
