package xyz.akumano.kafka.rawride;

import org.apache.commons.cli.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import xyz.akumano.kafka.common.KafkaTopicConsumer;

public class RawRideConsumer extends KafkaTopicConsumer<String, RawRide> {

    public RawRideConsumer(String topicName,
                           String groupId,
                           String bootstrapServer) {
        super(topicName, groupId, bootstrapServer,
                StringDeserializer.class, RawRide.RawRideDeserializer.class);
    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        Option topicNameOption = Option.builder()
                .longOpt("topicName")
                .hasArg().required()
                .desc("The name of a topic to which raw rides are sent.")
                .build(),
          bootstrapServerOption = Option.builder()
            .longOpt("bootstrapServer")
            .hasArg().required()
            .desc("The uri of a Kafka bootstrap server.")
            .build();

        options.addOption(topicNameOption);
        options.addOption(bootstrapServerOption);

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        //String topicName = "raw-rides-2014-05-small";
        //String topicName = "raw-rides-test";
        String topicName = cmd.getOptionValue(topicNameOption);

        String groupId = "raw-ride-consumer-local";
        String bootstrapServer = cmd.getOptionValue(bootstrapServerOption);
        KafkaTopicConsumer consumer = new RawRideConsumer(topicName, groupId, bootstrapServer);

        System.out.println( consumer.getEndOffset() );

        //Iterator<RawRide> rides = trc.getRecords();
        int n = 20;
        Iterable<RawRide> rs = consumer.getNRecordsList(n);
        System.out.printf("Showing the first %d records", n);
        for (RawRide r : rs) System.out.println(r);

        consumer.close();
        System.out.println("Consumer has stopped");
    }
}
