package xyz.akumano.beam.popularareas;

import avro.shaded.com.google.common.collect.Lists;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import xyz.akumano.kafka.common.KafkaTopicConsumer;

public class CellCountConsumer extends KafkaTopicConsumer<Integer, CellCount> {
    public CellCountConsumer(String topicName,
                             String groupId,
                             String bootstrapServer) {
        super(topicName, groupId, bootstrapServer,
                IntegerDeserializer.class, CellCount.CellCountDeserializer.class);
    }
    public static void main(String[] args) {
        String topicName = "popular-cells-2014-05-small";
        String groupId = "popular-cell-consumer-local";
        String bootstrapServer = "localhost:9092";
        CellCountConsumer consumer = new CellCountConsumer(topicName, groupId, bootstrapServer);
        Iterable<CellCount> rs = Lists.newArrayList(consumer.getRecords());
        for (CellCount r : rs) System.out.println(r);
        consumer.close();
    }
}
