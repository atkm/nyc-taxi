package xyz.akumano.kafka.rawride;

import org.apache.commons.cli.*;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import xyz.akumano.kafka.common.CsvProducer;

import java.util.*;
import java.util.concurrent.Future;

// ref: http://aseigneurin.github.io/2016/03/04/kafka-spark-avro-producing-and-consuming-avro-messages.html

public class RawRideProducer extends CsvProducer<String, RawRide> {
    /*** ----
     * Raw ride data format
     vendor_id, pickup_datetime, dropoff_datetime, passenger_count, trip_distance, pickup_longitude, pickup_latitude, rate_code, store_and_fwd_flag, dropoff_longitude, dropoff_latitude, payment_type, fare_amount, surcharge, mta_tax, tip_amount, tolls_amount, total_amount
     CMT,2014-05-07 09:33:52,2014-05-07 09:52:36,1,1.8,-73.977132999999995,40.747166999999997,1,N,-73.981178,40.765351000000003,CRD,12.5,0,0.5,2.6000000000000001,0,15.6

     columns[1] = pickup_datetime,
     columns[6] = pickup_latitude,
     columns[5] = pickup_longitude
     */
    // use null for key (will use zip when the data is enriched with it).

    // csvPath is read with RawRideProducer.class.getResourceAsStream
    public RawRideProducer(String topicName, String cliendId, String bootstrapServer) {
        super(topicName, cliendId, bootstrapServer,
                StringSerializer.class, RawRide.RawRideSerializer.class);
    }
    protected String getKey(RawRide ride) {
        return null;
    }

    // takes a Scanner of a csv file and returns an iterator of
    private class RideIterator implements Iterator<RawRide> {

        private Scanner sc;
        RideIterator(Scanner sc) {
            // Grab the header, get it out of the way.
            String header = sc.nextLine();
            //String[] columns = header.split(", ");
            this.sc = sc;
        }

        @Override
        public RawRide next() {
            String[] row = sc.nextLine().split(",");
            RawRide ride = new RawRide(row[1], Double.valueOf(row[6]), Double.valueOf(row[5]));
            return ride;
        }

        @Override
        public boolean hasNext() {
            return sc.hasNext();
        }
    }
    protected Iterator<RawRide> recordIterator(Scanner sc) {
        return new RideIterator(sc);
    }

    @Override
    public Future<RecordMetadata> send(String csvPath) {
        Future<RecordMetadata> sendResult = null;
        try (Scanner sc = csvScanner(csvPath)) {
            RideIterator itr = new RideIterator(sc);
            int badCoordinates = 0;
            while (itr.hasNext()) {
                RawRide ride = itr.next();
                double lat = ride.getPickupLatitude(), lon = ride.getPickupLongitude();
                if (badData(lat, lon)) {
                    badCoordinates++;
                } else {
                    sendResult = sendOne(ride);
                }
            }
            System.out.printf("There were %d bad pickup coordinates.%n", badCoordinates);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sendResult;
    }

    public boolean badData(Double lat, Double lon) {
        return lat.equals(0.0) | lon.equals(0.0);
    }

    public static void main( String[] args ) throws Exception {

        Options options = new Options();
        Option topicNameOption = Option.builder()
                .longOpt("topicName")
                .hasArg().required()
                .desc("The name of a topic to which raw rides are sent.")
                .build(),
        csvPathOption = Option.builder()
                .longOpt("csvPath")
                .hasArg().required()
                .desc("The uri of a csv file containing the rides to send.")
                .build(),
        bootstrapServerOption = Option.builder()
                .longOpt("bootstrapServer")
                .hasArg().required()
                .desc("The uri of a Kafka bootstrap server.")
                .build();

        options.addOption(topicNameOption);
        options.addOption(csvPathOption);
        options.addOption(bootstrapServerOption);

        if (args.length == 0) {
            HelpFormatter help = new HelpFormatter();
            help.printHelp("RawRideProducer", options);
        }


        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        String topicName = cmd.getOptionValue(topicNameOption);
        //String topicName = "raw-rides-2014-05-small";
        //String topicName = "raw-rides-2014-05-tiny";
        String clientId = "raw-ride-producer-local";

        String csvPath = cmd.getOptionValue(csvPathOption);
        //String csvPath = "/yellow_tripdata_2014-05-sorted-small.csv";
        //String csvPath = "/yellow_tripdata_2014-05-tiny.csv";

        String bootstrapServer = cmd.getOptionValue(bootstrapServerOption);
        //String bootstrapServer = "localhost:9092";

        // clear topic before adding
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServer);
        AdminClient admin = AdminClient.create(props);
        admin.deleteTopics(Collections.singletonList(topicName));

        RawRideProducer producer = new RawRideProducer(topicName, clientId, bootstrapServer);
        producer.send(csvPath);
        producer.close();
    }
}
