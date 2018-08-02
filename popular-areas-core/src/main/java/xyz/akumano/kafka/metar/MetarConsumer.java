package xyz.akumano.kafka.metar;

import org.apache.kafka.common.serialization.StringDeserializer;
import xyz.akumano.kafka.common.KafkaTopicConsumer;

public class MetarConsumer extends KafkaTopicConsumer<String, Metar> {
    public MetarConsumer(String topicName,
                           String groupId,
                           String bootstrapServer) {
        super(topicName, groupId, bootstrapServer,
                StringDeserializer.class, Metar.MetarDeserializer.class);
    }

    public static void main(String[] args) {
        String topicName = "metar-2014-05";
        String groupId = "metar-consumer-local";
        String bootstrapServer = "localhost:9092";
        KafkaTopicConsumer consumer = new MetarConsumer(topicName, groupId, bootstrapServer);

        System.out.println( consumer.getEndOffset() );

        //Iterator<Metar> rides = trc.getRecords();
        int n = 20;
        Iterable<Metar> rs = consumer.getNRecordsList(n);
        System.out.printf("Showing the first %d records", n);
        for (Metar r : rs) System.out.println(r);

        consumer.close();
        System.out.println("Consumer has stopped");
    }
}
