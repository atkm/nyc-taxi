package xyz.akumano.kafka;

import xyz.akumano.kafka.rawride.RawRide;
import xyz.akumano.kafka.rawride.RawRideConsumer;
import xyz.akumano.kafka.rawride.RawRideProducer;

public class RawRideProducerTest extends CsvProducerTest<RawRide> {
    @Override
    public String setTopicName() {
        return "raw-rides-tests";
    }
    @Override
    public String setGroupId() {
        return "raw-rides-tests-consumer";
    }
    @Override
    public String setClientId() {
        return "raw-rides-tests-client";
    }
    @Override
    public String setBootstrapServer() {
        return "localhost:9092";
    }
    @Override
    public String setCsvPath() {
        return "/yellow_tripdata_2014-05-tiny.csv";
    }
    @Override
    public RawRideProducer createProducer(String topicName, String clientId, String bootstrapServer) {
        return new RawRideProducer(topicName, clientId, bootstrapServer);
    };
    @Override
    public RawRideConsumer createConsumer(String topicName, String groupId, String bootstrapServer) {
        return new RawRideConsumer(topicName, groupId, bootstrapServer);
    }

}
