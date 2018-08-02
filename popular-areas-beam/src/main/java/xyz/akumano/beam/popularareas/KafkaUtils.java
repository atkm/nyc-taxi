package xyz.akumano.beam.popularareas;

import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.TimestampPolicyFactory;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class KafkaUtils {

    /**
     * Creates a topic if it does not exist, and flushes it if it exists.
     * @param topicName
     * @param bootstrapServer
     */
    public static void clearTopic(String topicName, String bootstrapServer) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServer);
        AdminClient admin = AdminClient.create(props);
        admin.deleteTopics(Collections.singletonList(topicName));
    }

    public static class TopicReader<K, V> extends PTransform<PBegin, PCollection<KV<K, V>>> {
        private final String bootstrapServer, sourceTopicName;
        private final Map<String, Object> consumerConfig;
        private final Class<? extends Deserializer<K>> keyDeserializerClass;
        private final Class<? extends Deserializer<V>> valueDeserializerClass;
        private final TimestampPolicyFactory<K,V> tpFactory;

        public TopicReader(String sourceTopicName,
                           String bootstrapServer,
                           Map<String, Object> consumerConfig,
                           Class keyDeserializerClass,
                           Class valueDeserializerClass
        ) {
            this.sourceTopicName = sourceTopicName;
            this.bootstrapServer = bootstrapServer;
            this.consumerConfig = consumerConfig;
            this.keyDeserializerClass = keyDeserializerClass;
            this.valueDeserializerClass = valueDeserializerClass;
            this.tpFactory = null;
        }
        public TopicReader(String sourceTopicName,
                           String bootstrapServer,
                           Map<String, Object> consumerConfig,
                           Class keyDeserializerClass,
                           Class valueDeserializerClass,
                           TimestampPolicyFactory tpFactory
        ) {
            this.sourceTopicName = sourceTopicName;
            this.bootstrapServer = bootstrapServer;
            this.consumerConfig = consumerConfig;
            this.keyDeserializerClass = keyDeserializerClass;
            this.valueDeserializerClass = valueDeserializerClass;
            this.tpFactory = tpFactory;
        }
        @Override
        public PCollection<KV<K,V>> expand(PBegin pBegin) {
            if (tpFactory == null)
                return pBegin.apply(KafkaIO.<K, V>read()
                        .withBootstrapServers(bootstrapServer)
                        .withTopic(sourceTopicName)
                        .withKeyDeserializer(keyDeserializerClass)
                        .withValueDeserializer(valueDeserializerClass)
                        .updateConsumerProperties(consumerConfig)
                        .withoutMetadata()
                );
            else
                return pBegin.apply(KafkaIO.<K, V>read()
                        .withBootstrapServers(bootstrapServer)
                        .withTopic(sourceTopicName)
                        .withKeyDeserializer(keyDeserializerClass)
                        .withValueDeserializer(valueDeserializerClass)
                        .updateConsumerProperties(consumerConfig)
                        .withTimestampPolicyFactory(tpFactory)
                        .withoutMetadata()
                );
        }
    }

    public static class TopicKeyReader<K> extends PTransform<PBegin, PCollection<K>> {
        private final String bootstrapServer, sourceTopicName;
        private final Map<String, Object> consumerConfig;
        private final Class<? extends Deserializer<K>> keyDeserializerClass;

        public TopicKeyReader(String sourceTopicName,
                              String bootstrapServer,
                              Map<String, Object> consumerConfig,
                              Class keyDeserializerClass
        ) {
            this.sourceTopicName = sourceTopicName;
            this.bootstrapServer = bootstrapServer;
            this.consumerConfig = consumerConfig;
            this.keyDeserializerClass = keyDeserializerClass;
        }

        @Override
        public PCollection<K> expand(PBegin pBegin) {
            return pBegin.apply(KafkaIO.<K, String>read()
                    .withBootstrapServers(bootstrapServer)
                    .withTopic(sourceTopicName)
                    .withKeyDeserializer(keyDeserializerClass)
                    .withValueDeserializer(StringDeserializer.class)
                    .updateConsumerProperties(consumerConfig)
                    .withoutMetadata()
            )
                    .apply(Keys.<K>create());
        }
    }

    public static class TopicWriter<K, V> extends PTransform<PCollection<KV<K, V>>, PDone> {
        private final String bootstrapServer, sinkTopicName;
        private final Class<? extends Serializer<K>> keySerializerClass;
        private final Class<? extends Serializer<V>> valueSerializerClass;

        public TopicWriter(String sinkTopicName,
                           String bootstrapServer,
                           Class keySerializerClass,
                           Class valueSerializerClass
        ) {
            this.sinkTopicName = sinkTopicName;
            this.bootstrapServer = bootstrapServer;
            this.keySerializerClass = keySerializerClass;
            this.valueSerializerClass = valueSerializerClass;
        }

        @Override
        public PDone expand(PCollection<KV<K,V>> records) {
            return records.apply(KafkaIO.<K, V>write()
                    .withBootstrapServers(bootstrapServer)
                    .withTopic(sinkTopicName)
                    .withKeySerializer(keySerializerClass)
                    .withValueSerializer(valueSerializerClass)
            );
        }
    }
}

