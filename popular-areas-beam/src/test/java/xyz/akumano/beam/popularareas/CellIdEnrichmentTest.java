package xyz.akumano.beam.popularareas;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import xyz.akumano.kafka.rawride.RawRide;
import xyz.akumano.kafka.rawride.RawRideProducer;

import java.util.*;

import org.junit.*;
import static org.junit.Assert.*;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;

/** GridUtils tests:
 * - a ride close to (LAT_NORTH, LAT_WEST) has id = 0
 * - a ride close to (LAT_NORTH, LAT_EAST) has id = NGRID_X - 1
 * - a ride close to (LAT_SOUTH, LAT_WEST) has id = (NGRID_Y - 1) * NGRID_X
 * - a ride close to (LAT_SOUTH, LAT_EAST) has id = NGRID_Y * NGRID_X - 1
 * - a ride close to (LAT_NORTH + 3/2 * DELTA_Y, LAT_WEST) (i.e. the cell below the top-left cell) has id = NGRID_X
 */

public class CellIdEnrichmentTest {

    @Rule
    public final transient TestPipeline p = TestPipeline.create();
    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none();

    // constants copied from GeoUtils.scala -- TODO: statically import GeoUtils to get the constants.
    private static float LAT_NORTH = (float) 41.0, LAT_SOUTH = (float) 40.5,
            LAT_WEST = (float) -74.05, LAT_EAST = (float) -73.70,
            LAT_HEIGHT = LAT_NORTH - LAT_SOUTH,
            LON_WIDTH = LAT_EAST - LAT_WEST;
    private static int NGRID_Y = 400, NGRID_X = 250;
    private double DELTA_Y = LAT_HEIGHT/NGRID_Y, DELTA_X = LON_WIDTH/NGRID_X;

    RawRide bottomLeftRide = new RawRide("", LAT_SOUTH + DELTA_Y/1.5, LAT_WEST + DELTA_Y/2);
    RawRide bottomRightRide = new RawRide("", LAT_SOUTH + DELTA_Y/30, LAT_EAST - DELTA_Y/1.1);
    RawRide topRightRide = new RawRide("", LAT_NORTH - DELTA_Y/10, LAT_EAST - DELTA_Y/2);
    RawRide topLeftRide = new RawRide("", LAT_NORTH - DELTA_Y/10, LAT_WEST + DELTA_Y/20);


    // Test integration with Kafka
    @Test
    public void enrichmentPipelineTest() throws Exception {
        //exit.expectSystemExit();
        exit.expectSystemExitWithStatus(0);

        String sourceTopicName = "raw-rides-test";
        String clientId = "raw-ride-test-producer";
        String sinkTopicName = "enriched-rides-test";
        String groupId = "enriched-ride-test-consumer";
        String bootstrapServer = "localhost:9092";

        DirectOptions options = PipelineOptionsFactory.as(DirectOptions.class);
        options.setBlockOnRun(false);
        Pipeline pipeline = Pipeline.create(options);
        Map<String, Object> consumerConfig = ImmutableMap.of("auto.offset.reset", "earliest");

        KafkaUtils.clearTopic(sourceTopicName, bootstrapServer);
        KafkaUtils.clearTopic(sinkTopicName, bootstrapServer);


        pipeline.apply(new CellIdEnrichment.EnrichmentTransform(
                sourceTopicName, sinkTopicName, bootstrapServer, consumerConfig));

        PipelineResult pipelineResult = pipeline.run();

        RawRideProducer producer = new RawRideProducer(sourceTopicName, clientId, bootstrapServer);
        List<RawRide> rides = ImmutableList.of(topLeftRide, topRightRide, bottomLeftRide, bottomRightRide);
        producer.send(rides).get();
        Set<CelledRide> enrichedRides = ImmutableSet.of(
                new CelledRide("", LAT_NORTH - DELTA_Y/10, LAT_WEST + DELTA_Y/20, 0),
                new CelledRide("", LAT_NORTH - DELTA_Y/10, LAT_EAST - DELTA_Y/2, NGRID_X - 1),
                new CelledRide("", LAT_SOUTH + DELTA_Y/1.5, LAT_WEST + DELTA_Y/2, NGRID_X * (NGRID_Y - 1)),
                new CelledRide("", LAT_SOUTH + DELTA_Y/30, LAT_EAST - DELTA_Y/1.1, NGRID_X * NGRID_Y - 1)
        );

        CelledRideConsumer consumer = new CelledRideConsumer(sinkTopicName, groupId, bootstrapServer);
        Set<CelledRide> returnedRides = Sets.newHashSet(consumer.getRecords());
        assertEquals(enrichedRides, returnedRides);

        producer.close();
        consumer.close();
        // exit to kill the thread running the pipeline along with the main process.
        System.exit(0);
    }

    // GeoUtils tests
   @Test
    public void topLeftGridTest() {
        PCollection<RawRide> input = p.apply(Create.of(topLeftRide));
        PCollection<Integer> output = input.apply(ParDo.of(new CellIdEnrichment.CellIdFn()))
                .apply(Keys.create());
        PAssert.that(output).containsInAnyOrder(0);
        p.run();
    }
    @Test
    public void topRightGridTest() {
        PCollection<RawRide> input = p.apply(Create.of(topRightRide));
        PCollection<Integer> output = input.apply(ParDo.of(new CellIdEnrichment.CellIdFn()))
                .apply(Keys.create());
        PAssert.that(output).containsInAnyOrder(NGRID_X-1);
        p.run();
    }
    @Test
    public void bottomLeftGridTest() {
        PCollection<RawRide> input = p.apply(Create.of(bottomLeftRide));
        PCollection<Integer> output = input.apply(ParDo.of(new CellIdEnrichment.CellIdFn()))
                .apply(Keys.create());
        PAssert.that(output).containsInAnyOrder((NGRID_Y - 1) * NGRID_X);
        p.run();
    }
    @Test
    public void bottomRightGridTest() {
        PCollection<RawRide> input = p.apply(Create.of(bottomRightRide));
        PCollection<Integer> output = input.apply(ParDo.of(new CellIdEnrichment.CellIdFn()))
                .apply(Keys.create());
        PAssert.that(output).containsInAnyOrder(NGRID_Y*NGRID_X - 1);
        p.run();
    }
    @Test
    public void secondRowGridTest() {
        PCollection<RawRide> input = p.apply(Create.of(
                new RawRide("", LAT_NORTH - 3*DELTA_Y/2, LAT_WEST + DELTA_Y/1.1)
                )
        );
        PCollection<Integer> output = input.apply(ParDo.of(new CellIdEnrichment.CellIdFn()))
                .apply(Keys.create());
        PAssert.that(output).containsInAnyOrder(NGRID_X);
        p.run();
    }
    @Test
    public void proximatePointsTest() {
        float lat = LAT_NORTH - (float)(103*DELTA_Y + DELTA_Y/2);
        float lon = LAT_WEST + (float)(29*DELTA_X + DELTA_X/2);
        int id = GeoUtils.toGridCell(new GeoUtils.Coord(lat, lon));
        RawRide ride1 = new RawRide("", lat, lon); // cell coordinate = (103, 29)
        // a ride near ride1
        RawRide ride2 = new RawRide("", lat + DELTA_Y/20, lon - DELTA_X/30);
        PCollection<RawRide> input = p.apply(Create.of(ride1, ride2));
        PCollection<Integer> output = input.apply(ParDo.of(new CellIdEnrichment.CellIdFn()))
                .apply(Keys.create());
        PAssert.that(output).containsInAnyOrder(id, id);
        p.run();
    }

}
