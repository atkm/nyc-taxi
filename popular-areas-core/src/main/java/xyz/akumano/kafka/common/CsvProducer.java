package xyz.akumano.kafka.common;

import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.Future;

// parses a csv file, and sends the content to a kafka topic.
// V: the type of object to be sent
public abstract class CsvProducer<K, V extends AvroSerializable> extends KafkaTopicProducer<K, V> {

    // TODO: should the constructor just take Properties, since I will eventually want to customize consumers in a fine-grain manner?
    /**
     * @param topicName
     * @param cliendId
     * @param bootstrapServer
     * @param keySerializerClass
     * @param valueSerializerClass the Class of serializer for V
     */
    public CsvProducer(String topicName, String cliendId, String bootstrapServer,
                       Class keySerializerClass, Class valueSerializerClass) {
        super(topicName, cliendId, bootstrapServer, keySerializerClass, valueSerializerClass);
    }

    /** sends an object of type V to a kafka topic
     * @return The RecordMetadata returned from the final send operation.
     */
    public Future<RecordMetadata> send(String csvPath) {
        Scanner sc = csvScanner(csvPath);
        Iterator<V> itr = recordIterator(sc);
        return send(itr);
    }

    // For testing. Returns rides that will be sent with the send method.
    public List<V> toBeSent(String csvPath) {
        Scanner sc = csvScanner(csvPath);
        Iterator<V> itr = recordIterator(sc);
        return toBeSent(itr);
    };

    /**
     * @param sc a Scanner of csv
     * @return an iterator of objects corresponding to the rows of the csv file.
     */
    abstract protected Iterator<V> recordIterator(Scanner sc);

    /**
     * @param csvPath the path to a csv resource. To get "/src/main/resources/file.csv",
     *               for example, set this parameter to "/file.csv".
     * @return a Scanner for the given csv resource.
      */
    protected Scanner csvScanner(String csvPath) {
        // This works as well.
        //String csvPath = this.getClass().getResource(csvFile).getPath();
        // or this
        //String csvPath = RawRideProducer.class.getResource(csvFile).getPath();
        //InputStream csvStream = RawRideProducer.class.getResourceAsStream(csvPath);

        System.out.printf("Reading from %s%n", csvPath);
        InputStream csvStream = CsvProducer.class.getResourceAsStream(csvPath);
        // if such resource does not exist.
        if (csvStream == null)
            try {
                csvStream = new FileInputStream(csvPath);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        String line = "";
        Scanner sc = new Scanner(csvStream);
        return sc;
    }
}
