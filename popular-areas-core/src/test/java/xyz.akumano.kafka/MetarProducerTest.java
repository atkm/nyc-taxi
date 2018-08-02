package xyz.akumano.kafka;

import xyz.akumano.kafka.metar.Metar;
import xyz.akumano.kafka.metar.MetarConsumer;
import xyz.akumano.kafka.metar.MetarProducer;

public class MetarProducerTest extends CsvProducerTest<Metar> {
    @Override
    public String setTopicName() {
        return "raw-metar-tests";
    }
    @Override
    public String setGroupId() {
        return "raw-metar-tests-consumer";
    }
    @Override
    public String setClientId() {
        return "raw-metar-tests-client";
    }
    @Override
    public String setBootstrapServer() {
        return "localhost:9092";
    }
    @Override
    public String setCsvPath() {
        return "/lga-2014-05-metar.csv";
    }
    @Override
    public MetarProducer createProducer(String topicName, String clientId, String bootstrapServer) {
        return new MetarProducer(topicName, clientId, bootstrapServer);
    };
    @Override
    public MetarConsumer createConsumer(String topicName, String groupId, String bootstrapServer) {
        return new MetarConsumer(topicName, groupId, bootstrapServer);
    }

}
