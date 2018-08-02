package xyz.akumano.kafka.common;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

// A simple consumer class that sends a finite number of records to a topic.
abstract public class KafkaTopicProducer<K, V extends AvroSerializable> {

    private final String topicName, bootstrapServer, clientId;
    private final Class keySerializerClass, valueSerializerClass;
    private final Producer<K, V> producer;

    /**
     * @param topicName
     * @param cliendId
     * @param bootstrapServer
     * @param keySerializerClass the Class of serializer for K
     * @param valueSerializerClass the Class of serializer for V
     *
     **/
    public KafkaTopicProducer(String topicName, String cliendId, String bootstrapServer,
                              Class keySerializerClass, Class valueSerializerClass) {
        this.topicName = topicName;
        this.clientId = cliendId;
        this.bootstrapServer = bootstrapServer;
        this.keySerializerClass = keySerializerClass;
        this.valueSerializerClass = valueSerializerClass;
        this.producer = makeProducer();
    }

    private Producer<K, V> makeProducer() {
        Properties props = new Properties();
        props.put("client.id", clientId);
        props.put("producer.type", "sync");
        props.put("acks", "all");
        props.put("bootstrap.servers", bootstrapServer);
        props.put("key.serializer", keySerializerClass);
        props.put("value.serializer", valueSerializerClass);

        Producer<K, V> producer = new KafkaProducer<>(props);
        return producer;
    }
    public Producer<K, V> getProducer() {
        return producer;
    }

    /** sends an instance of type V to a kafka topic
     * @param instance
     * @return The RecordMetadata returned from the send operation.
     */
    public Future<RecordMetadata> sendOne(V instance) {
        K key = getKey(instance);
        return sendOne(key, instance);
    }
    private Future<RecordMetadata> sendOne(K key, V instance) {
        ProducerRecord<K, V> record = new ProducerRecord<>(topicName, key, instance);
        return producer.send(record);
    };

    /**
     * Get a key from an instance.
     * @param instance
     */
    abstract protected K getKey(V instance);

    /** sends multiple instances of type V to a kafka topic
     * @param itr An iterator of instances to send.
     * @return The RecordMetadata returned from the send operation of the last element in the iterator.
     */
    public Future<RecordMetadata> send(Iterator<V> itr) {
        Future<RecordMetadata> sendResult = null;
        while (itr.hasNext())
            sendResult = sendOne(itr.next());
        return sendResult;
    }
    public Future<RecordMetadata> send(Iterable<V> records) {
        Future<RecordMetadata> sendResult = null;
        for (V r : records)
            sendResult = sendOne(r);
        return sendResult;
    }
    /**
     * @param itr An iterator of instances to send.
     * @return a list of elements that would be sent when send(itr) is called.
     */
    public List<V> toBeSent(Iterator<V> itr) {
        List<V> ls = new ArrayList<>();
        itr.forEachRemaining(ls::add);
        return ls;
    }

    /** closes the underlying KafkaProducer
     */
    public void close() {
        producer.close();
    };

}
