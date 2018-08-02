package xyz.akumano.beam.popularareas;

import com.google.common.annotations.VisibleForTesting;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.joda.time.Duration;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

// send (datetime, cellId, count) to Kafka. Key by cell id.
// see Main.java for a pipeline writing to ES
public class PopularCells {

    @VisibleForTesting
    static class CountByCellId extends PTransform<PCollection<Integer>, PCollection<KV<Integer, CellCount>>> {

        private final long windowSize, windowFrequency;

        public CountByCellId(long windowSize, long windowFrequency) {
            this.windowSize = windowSize;
            this.windowFrequency = windowFrequency;
        }

        @Override
        public PCollection<KV<Integer, CellCount>> expand(PCollection<Integer> cellIds) {
            PCollection<Integer> windowedIds = cellIds.apply(Window.<Integer>into(
                    SlidingWindows.of(Duration.standardMinutes(windowSize))
                            .every(Duration.standardMinutes(windowFrequency))
                    )
            );
            PCollection<KV<Integer, Long>> countsByCellId = windowedIds.apply(Count.<Integer>perElement());
            PCollection<KV<Integer, CellCount>> cellCounts = countsByCellId.apply(ParDo.of(new CountToCellCount()));
            return cellCounts;
        }
    }

    /**
     * Given a window, cell id, and count associated with the cell, creates a CellCount instance,
     * and returns a KV of the same cell id and the CellCount instance.
     */
    private static class CountToCellCount extends DoFn<KV<Integer, Long>, KV<Integer, CellCount>> {
        @ProcessElement
        public void processElement(ProcessContext ctx, BoundedWindow window) {
            KV<Integer, Long> pair = ctx.element();
            int cellId = pair.getKey();
            String datetime = window.maxTimestamp().toString();
            CellCount cellCount = new CellCount(datetime, cellId, pair.getValue());
            ctx.output(KV.of(cellId, cellCount));
        }
    }

    /**
     * Emit only the IDs whose count is greater than or equal to the threshold.
     */
    public static class FilterPopularCells extends DoFn<KV<Integer, CellCount>, KV<Integer, CellCount>> {
        private int threshold;
        public FilterPopularCells(int threshold) {
            this.threshold = threshold;
        }

        @ProcessElement
        public void processElement(ProcessContext ctx) {
            KV<Integer, CellCount> pair = ctx.element();
            if (pair.getValue().getCount() >= threshold)
                ctx.output(pair);
        }
    }


    public static class PopularCellsTransform extends PTransform<PBegin, PDone> {
        private final String sourceTopicName, sinkTopicName, bootstrapServer;
        private final Map<String, Object> consumerConfig;
        private final int windowSize, windowFrequency, popularityThreshold;

        public PopularCellsTransform(String sourceTopicName,
                                     String sinkTopicName,
                                     String bootstrapServer,
                                     Map<String, Object> consumerConfig,
                                     int windowSize,
                                     int windowFrequency,
                                     int popularityThreshold) {
            this.sourceTopicName = sourceTopicName;
            this.sinkTopicName = sinkTopicName;
            this.bootstrapServer = bootstrapServer;
            this.consumerConfig = consumerConfig;
            this.windowSize = windowSize;
            this.windowFrequency = windowFrequency;
            this.popularityThreshold = popularityThreshold;
        }

        @Override
        public PDone expand(PBegin pBegin) {
            PCollection<Integer> rideCellIds = pBegin.apply(
                    new KafkaUtils.TopicReader<Integer, CelledRide>(sourceTopicName, bootstrapServer,
                            consumerConfig, IntegerDeserializer.class, CelledRide.CelledRideDeserializer.class,
                            new PickupTimePolicyFactory())
            )
                    .apply(Keys.<Integer>create());

            PCollection<KV<Integer, CellCount>> rideCounts = rideCellIds.apply(
                    new CountByCellId(windowSize, windowFrequency));

            PCollection<KV<Integer, CellCount>> popularCells = rideCounts.apply(ParDo.of(new FilterPopularCells(popularityThreshold)));

            return popularCells.apply(new KafkaUtils.TopicWriter<Integer, CellCount>(
                    sinkTopicName, bootstrapServer, IntegerSerializer.class, CellCount.CellCountSerializer.class
            ));
        }
    }

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        String sourceTopicName = "enriched-rides-2014-05-small";
        //String sourceTopicName = "enriched-rides-test";
        String sinkTopicName = "popular-cells-2014-05-small";
        //String sinkTopicName = "popular-zipcodes-test";
        String bootstrapServer = "localhost:9092";

        Map<String, Object> consumerConfig = new HashMap<>();
        consumerConfig.put("auto.offset.reset", "earliest");

        // clear the sink topic
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        AdminClient admin = AdminClient.create(props);
        admin.deleteTopics(Collections.singletonList(sinkTopicName));

        pipeline.apply(new PopularCellsTransform(
                sourceTopicName, sinkTopicName, bootstrapServer, consumerConfig,
                15, 5, 20
        ));



        pipeline.run().waitUntilFinish();
    }
}
