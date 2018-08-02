package xyz.akumano.kafka.common;

import avro.shaded.com.google.common.collect.Iterators;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

// A simple consumer class for testing. Read the 0th partition of a topic with the <K, V> type.
// The class assumes that there is a finite number of records in a topic.
public class KafkaTopicConsumer<K, V extends AvroSerializable> {

    private final String topicName, groupId, bootstrapServer;
    private final Consumer<K, V> consumer;
    private final Class keyDeserializerClass, valueDeserializerClass;

    // TODO: should the constructor take Properties, since I will eventually want to customize consumers in a fine-grain manner?
    public KafkaTopicConsumer(String topicName, String groupId, String bootstrapServer,
                              Class keyDeserializerClass, Class valueDeserializerClass) {
        this.topicName = topicName;
        this.groupId = groupId;
        this.bootstrapServer = bootstrapServer;
        this.keyDeserializerClass = keyDeserializerClass;
        this.valueDeserializerClass = valueDeserializerClass;
        this.consumer = topicConsumer();
    }

    // exposed to the client as an access point for inspecting the topic (e.g. offset, partitions).
    private Consumer<K, V> topicConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServer);
        props.put("group.id", groupId);
        props.put("key.deserializer", keyDeserializerClass);
        props.put("value.deserializer", valueDeserializerClass);

        Consumer<K, V> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topicName));
        return consumer;
    }

    // Warning: the reference to the consumer is leaked.
    public Consumer<K, V> getConsumer() {
        return consumer;
    }
    public void close() {
        consumer.close();
    }

    /**
     * Given a ConsumerRecords instance, returns an iterator of its values.
     */
    private class RecordsIterator implements Iterator<V> {

        Iterator<ConsumerRecord<K, V>> records;
        RecordsIterator(ConsumerRecords<K, V> records) {
            this.records = records.iterator();
        }
        @Override
        public V next() {
            ConsumerRecord<K, V> record = records.next();
            return record.value();
        }
        @Override
        public boolean hasNext() {
            return records.hasNext();
        }
    }

    /**
     * The method assumes that the topic is bounded.
     * @return an iterator of the values of all records in a topic.
     */
    public Iterator<V> getRecords() {
        // dummy poll for the lazy subscribe()
        ConsumerRecords<K, V> consumerRecords = consumer.poll(0);
        //System.out.println("dummy poll got " + consumerRecords.count() + " records");
        // TODO: not sure if this line does anything, as consumerRecords.partitions() is empty.
        consumer.seekToBeginning(consumerRecords.partitions());

        int noRecordsCount = 0, noRecordsThreshold = 4;
        Iterator<V> records = Iterators.emptyIterator();

        while (true) {
            consumerRecords = consumer.poll(500);

            // get out if there hasn't been anything to consume in a while.
            if (consumerRecords.isEmpty()) {
                //System.out.println("no records");
                if (noRecordsCount++ > noRecordsThreshold) break;
            } else {
                //System.out.println("got " + consumerRecords.count() + " records");
                records = Iterators.concat(records, new RecordsIterator(consumerRecords));
            }
        }
        return records;
    }

    // returns the first n records
    public List<V> getNRecordsList(int n) {
        Iterator<V> records = getRecords();
        List<V> ls = new LinkedList<>();
        int i = 0;
        while (records.hasNext() && i < n) {
            ls.add(records.next());
            i++;
        }
        return ls;
    }

    public Map<TopicPartition, Long> getEndOffset() {
        return consumer.endOffsets(Collections.singletonList(
                new TopicPartition(topicName, 0)
        ));
    }

}
