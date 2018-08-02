package xyz.akumano.beam.popularareas;

import com.google.common.collect.Lists;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import xyz.akumano.kafka.common.KafkaTopicConsumer;

public class CelledRideConsumer extends KafkaTopicConsumer<Integer, CelledRide> {
    public CelledRideConsumer(String topicName,
                              String groupId,
                              String bootstrapServer) {
        super(topicName, groupId, bootstrapServer,
                IntegerDeserializer.class, CelledRide.CelledRideDeserializer.class);
    }

    public static void main(String[] args) {
        String topicName = "enriched-rides-test";
        String groupId = "enriched-ride-consumer-local";
        String bootstrapServer = "localhost:9092";
        CelledRideConsumer consumer = new CelledRideConsumer(topicName, groupId, bootstrapServer);
        Iterable<CelledRide> rs = Lists.newArrayList(consumer.getRecords());
        for (CelledRide r : rs) System.out.println(r);
        consumer.close();

    }
}