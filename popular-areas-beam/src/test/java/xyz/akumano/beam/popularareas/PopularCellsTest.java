package xyz.akumano.beam.popularareas;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.*;
import static org.junit.Assert.*;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;
import xyz.akumano.kafka.common.KafkaTopicProducer;

import java.util.Map;
import java.util.Set;

public class PopularCellsTest {
    @Rule
    public final transient TestPipeline p = TestPipeline.create();
    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none();

    static Duration oneMin = Duration.standardMinutes(1);
    static Duration twoMins = Duration.standardMinutes(2);

    @Test
    public void popularCellsTestNoElement() {
        int windowSize = 15, windowFrequency = 5;
        Duration windowSizeMins = Duration.standardMinutes(windowSize);

        Instant windowBegin = new Instant(0);
        Instant windowEnd = windowBegin.plus(windowSizeMins);

        TestStream<Integer> cellIds = TestStream.create(BigEndianIntegerCoder.of())
                .advanceWatermarkTo(windowBegin)
                .advanceWatermarkTo(windowEnd)
                .advanceWatermarkToInfinity(); // complete the stream

        PCollection<KV<Integer, CellCount>> counts = p.apply(cellIds)
                .apply(new PopularCells.CountByCellId(windowSize, windowFrequency));

        IntervalWindow window1 = new IntervalWindow(windowBegin, windowEnd);

        PAssert.that(counts)
                .inWindow(window1)
                .empty();

        p.run();
    }

    /**
     * The element is contained in the first window, but not contained in the next sliding window.
     */
    @Test
    public void popularCellsTestOneElement() {
        int windowSize = 15, windowFrequency = 5;
        Duration windowFreqMins = Duration.standardMinutes(windowFrequency);
        Duration windowSizeMins = Duration.standardMinutes(windowSize);

        Instant windowBegin = new Instant(0);
        Instant eventTime = windowBegin.plus(twoMins);
        Instant windowEnd = windowBegin.plus(windowSizeMins);

        TestStream<Integer> cellIds = TestStream.create(BigEndianIntegerCoder.of())
                .advanceWatermarkTo(windowBegin)
                .addElements(TimestampedValue.of(10, eventTime ))
                .advanceWatermarkTo(windowEnd)
                .advanceWatermarkToInfinity();

        PCollection<KV<Integer, CellCount>> counts = p.apply(cellIds)
                .apply(new PopularCells.CountByCellId(windowSize, windowFrequency));

        IntervalWindow window1 = new IntervalWindow(windowBegin, windowEnd);
        IntervalWindow window2 =
                new IntervalWindow(windowBegin.plus(windowFreqMins), windowEnd.plus(windowFreqMins));

        PAssert.that(counts)
                .inWindow(window1)
                // Note: (1) Beam thinks in UTC.
                // (2) window.end() = windowEnd = window.maxTimestamp() + 1ms.
                // (3) the first window starts at Instant(0) ??
                .containsInAnyOrder(KV.of(10,
                        new CellCount(window1.maxTimestamp().toDateTime(DateTimeZone.UTC).toString(), 10, 1)));

        PAssert.that(counts)
                .inWindow(window2)
                .empty();

        p.run();
    }

    /**
     * The element is contained in the first window and the succeeding sliding window.
     */
    @Test
    public void popularCellsTestOneElement2() {
        int windowSize = 15, windowFrequency = 5;
        Duration windowFreqMins = Duration.standardMinutes(windowFrequency);
        Duration windowSizeMins = Duration.standardMinutes(windowSize);

        Instant window1Begin = new Instant(0);
        Instant eventTime = window1Begin.plus(Duration.standardMinutes(6));
        Instant window1End = window1Begin.plus(windowSizeMins);

        TestStream<Integer> cellIds = TestStream.create(BigEndianIntegerCoder.of())
                .advanceWatermarkTo(window1Begin)
                .addElements(TimestampedValue.of(10, eventTime ))
                .advanceWatermarkTo(window1End)
                .advanceWatermarkToInfinity();

        PCollection<KV<Integer, CellCount>> counts = p.apply(cellIds)
                .apply(new PopularCells.CountByCellId(windowSize, windowFrequency));

        IntervalWindow window1 = new IntervalWindow(window1Begin, window1End);
        IntervalWindow window2 =
                new IntervalWindow(window1Begin.plus(windowFreqMins), window1End.plus(windowFreqMins));

        PAssert.that(counts)
                .inWindow(window1)
                .containsInAnyOrder(KV.of(10,
                        new CellCount(window1.maxTimestamp().toDateTime(DateTimeZone.UTC).toString(), 10, 1)));

        PAssert.that(counts)
                .inWindow(window2)
                .containsInAnyOrder(KV.of(10,
                        new CellCount(window2.maxTimestamp().toDateTime(DateTimeZone.UTC).toString(), 10, 1)));

        p.run();
    }

    @Test
    public void popularCellsTest1() {
        int windowSize = 15, windowFrequency = 5;
        Duration windowFreqMins = Duration.standardMinutes(windowFrequency);
        Duration windowSizeMins = Duration.standardMinutes(windowSize);

        Instant window1Begin = new Instant(0);
        Instant eventTime = window1Begin.plus(Duration.standardMinutes(6));
        Instant window1End = window1Begin.plus(windowSizeMins);

        TestStream<Integer> cellIds = TestStream.create(BigEndianIntegerCoder.of())
                .advanceWatermarkTo(window1Begin)
                .addElements(TimestampedValue.of(10, eventTime ))
                .addElements(TimestampedValue.of(10, eventTime ))
                .addElements(TimestampedValue.of(10, eventTime ))
                .advanceWatermarkTo(window1End)
                .addElements(TimestampedValue.of(10, eventTime.plus(Duration.standardMinutes(11)) ))
                .addElements(TimestampedValue.of(10, eventTime.plus(Duration.standardMinutes(12)) ))
                .advanceWatermarkToInfinity();

        PCollection<KV<Integer, CellCount>> counts = p.apply(cellIds)
                .apply(new PopularCells.CountByCellId(windowSize, windowFrequency));

        IntervalWindow window1 = new IntervalWindow(window1Begin, window1End);
        IntervalWindow window2 =
                new IntervalWindow(window1Begin.plus(windowFreqMins), window1End.plus(windowFreqMins));
        IntervalWindow window3 =
                new IntervalWindow(window2.start().plus(windowFreqMins), window2.end().plus(windowFreqMins));

        PAssert.that(counts)
                .inWindow(window1)
                .containsInAnyOrder(KV.of(10,
                        new CellCount(window1.maxTimestamp().toDateTime(DateTimeZone.UTC).toString(), 10, 3)));

        PAssert.that(counts)
                .inWindow(window2)
                .containsInAnyOrder(KV.of(10,
                        new CellCount(window2.maxTimestamp().toDateTime(DateTimeZone.UTC).toString(), 10, 5)));

        PAssert.that(counts)
                .inWindow(window3)
                .containsInAnyOrder(KV.of(10,
                        new CellCount(window3.maxTimestamp().toDateTime(DateTimeZone.UTC).toString(), 10, 2)));
        p.run();
    }

    static class CelledRideProducer extends KafkaTopicProducer<Integer, CelledRide> {
        public CelledRideProducer(String topicName, String clientId, String bootstrapServer) {
            super(topicName, clientId, bootstrapServer,
                    IntegerSerializer.class, CelledRide.CelledRideSerializer.class);
        }
        protected Integer getKey(CelledRide ride) {
            return ride.getPickupCellId();
        }
    }

    @Test
    public void popularCellsKafkaIntegrationTest() throws Exception {
        // TODO: currently, the test passes when Kafka is not running.
        exit.expectSystemExitWithStatus(0);

        String sourceTopicName = "enriched-rides-test";
        String clientId = "enriched-ride-test-producer";
        String sinkTopicName = "popular-cells-test";
        String groupId = "popular-cell-test-consumer";
        String bootstrapServer = "localhost:9092";

        DirectOptions options = PipelineOptionsFactory.as(DirectOptions.class);
        options.setBlockOnRun(false);
        Pipeline pipeline = Pipeline.create(options);
        Map<String, Object> consumerConfig = ImmutableMap.of("auto.offset.reset", "earliest");

        KafkaUtils.clearTopic(sourceTopicName, bootstrapServer);
        KafkaUtils.clearTopic(sinkTopicName, bootstrapServer);

        pipeline.apply(new PopularCells.PopularCellsTransform(sourceTopicName, sinkTopicName,
                bootstrapServer, consumerConfig, 15, 5, 3));

        PipelineResult pipelineResult = pipeline.run();

        CelledRideProducer producer = new CelledRideProducer(sourceTopicName, clientId, bootstrapServer);

        Set<CelledRide> rides = ImmutableSet.of(
                new CelledRide("2014-05-07 09:33:52", 0.0, 0.0, 10),
                new CelledRide("2014-05-07 09:33:53", 0.0, 0.0, 20019),
                new CelledRide("2014-05-07 09:33:54", 0.0, 0.0, 20019),
                new CelledRide("2014-05-07 09:33:55", 0.0, 0.0, 10),
                new CelledRide("2014-05-07 09:33:56", 0.0, 0.0, 20019),
                new CelledRide("2014-05-07 09:33:57", 0.0, 0.0, 10),
                new CelledRide("2014-05-07 09:33:59", 0.0, 0.0, 10),
                new CelledRide("2014-05-07 09:49:01", 0.0, 0.0, 10),
                new CelledRide("2014-05-07 11:49:01", 0.0, 0.0, 10)
        );
        producer.send(rides).get();

        CellCountConsumer consumer = new CellCountConsumer(sinkTopicName, groupId, bootstrapServer);
        // TODO: currently, the consumer only fetches from the 0th partition.
        Set<CellCount> returnedCounts = Sets.newHashSet(consumer.getRecords());
        returnedCounts.forEach(c -> System.out.println(c));
        assertTrue(returnedCounts.stream().anyMatch(count -> count.getCellId() == 10 && count.getCount() == 4));
        assertTrue(returnedCounts.stream().anyMatch(count -> count.getCellId() == 20019 && count.getCount() == 3));

        producer.close();
        consumer.close();
        System.exit(0);
    }

    // Integration with CellIdEnrichment.
    @Test
    public void popularCellsIntegrationTest() {
        throw new RuntimeException("not implemented");
    }
}
