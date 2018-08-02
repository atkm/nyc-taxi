package xyz.akumano.kafka;

import static java.util.Arrays.asList;

import org.junit.*;
import static org.junit.Assert.*;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.junit.runners.MethodSorters;
import xyz.akumano.kafka.common.CsvProducer;
import xyz.akumano.kafka.common.KafkaTopicConsumer;

import java.util.*;
import java.util.concurrent.ExecutionException;

@FixMethodOrder(MethodSorters.DEFAULT)
abstract public class CsvProducerTest<T> {

    private final String topicName = setTopicName();
    private final String groupId = setGroupId();
    private final String clientId = setClientId();
    private final String bootstrapServer = setBootstrapServer();
    private final String csvPath = setCsvPath();

    private final Collection<String> topicList = Collections.singletonList(topicName);
    // TODO: support topics that use multiple partitions.
    private final TopicPartition topicPartition = new TopicPartition(topicName, 0);

    abstract public String setTopicName();
    abstract public String setGroupId();
    abstract public String setClientId();
    abstract public String setCsvPath();
    abstract public String setBootstrapServer();
    abstract public KafkaTopicConsumer createConsumer(String topicName, String groupId, String bootstrapServer);
    abstract public CsvProducer createProducer(String topicName, String clientId, String bootstrapServer);

    // create the topic if it doesn't exist, and flush it before each test
    @Before
    public void topicSetup() {

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServer);

        AdminClient admin = AdminClient.create(props);
        createTopic(admin, topicName);
        admin.deleteTopics(topicList);
    }

    private static void createTopic(AdminClient admin, String topic) {
        try {
            Set<String> existingTopics = admin.listTopics().names().get();
            //System.out.println(existingTopics);
            if (!existingTopics.contains(topic)) {
                Map<String, String> configs = new HashMap<>();
                int partitions = 1;
                short replication = 1;
                admin.createTopics(asList(
                        new NewTopic(topic, partitions, replication).configs(configs))
                ).all().get();
            }
        } catch (InterruptedException | ExecutionException e) {
            System.err.println("error while creating raw-rides-test topic: " + e.getMessage());
        }

    }
    // As of Kafka 1.0.0, the broker option "delete.topic.enable" is set to true by default.
    @Test
    public void readFromEmptyTopic() {
        KafkaTopicConsumer consumer = createConsumer(topicName, groupId, bootstrapServer);
        assertEquals(Long.valueOf(0), consumer.getEndOffset().get(topicPartition));
    }

    @Test
    public void addRecordsToEmptyTopic() throws Exception
    {
        // mark the topic for deletion so it will be recreated upon producer.send()

        KafkaTopicConsumer consumer = createConsumer(topicName, groupId, bootstrapServer);
        CsvProducer producer = createProducer(topicName, clientId, bootstrapServer);

        List<T> ridesProduced = producer.toBeSent(csvPath);
        int numRides = ridesProduced.size();

        // send and wait for the operation to complete.
        producer.send(csvPath).get();

        assertEquals(Long.valueOf(numRides), consumer.getEndOffset().get(topicPartition));

        Iterator<T> rides = consumer.getRecords();
        int i = 0;
        while (rides.hasNext()) {
            assertEquals(rides.next(), ridesProduced.get(i++));
        }

        producer.close();
        consumer.close();
    }

    @Test
    public void addRecordsToNonEmptyTopic() throws Exception {

        KafkaTopicConsumer consumer = createConsumer(topicName, groupId, bootstrapServer);
        CsvProducer producer = createProducer(topicName, clientId, bootstrapServer);

        List<T> ridesProduced = producer.toBeSent(csvPath);
        int numRides = ridesProduced.size();

        // send the records twice.
        producer.send(csvPath);
        producer.send(csvPath);

        assertEquals(Long.valueOf(numRides * 2), consumer.getEndOffset().get(topicPartition));

        Iterator<T> rides = consumer.getRecords();
        int i = 0;
        while (rides.hasNext()) {
            assertEquals(rides.next(), ridesProduced.get(i%numRides));
            i++;
        }

        producer.close();
        consumer.close();
    }

}
