package gov.va.vba.kafka.streams.spring.demo.config;

import gov.va.vba.kafka.streams.spring.demo.processors.ClaimMessageProcessor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;
import java.util.UUID;

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.topic-name}")
    private String inputTopic;

    @Value("${spring.kafka.consumer.auto-offset-reset}")
    private String consumerAutoOffsetReset;

    @Value("${spring.kafka.consumer.batch-message-count}")
    private Integer batchMessageCount;

    @Value("${spring.kafka.consumer.wait-interval-ms}")
    private Integer waitInterval;

    private String processorClassName = ClaimMessageProcessor.class.getName();

    private Logger logger = LoggerFactory.getLogger(KafkaStreams.class);

    private static final String OUTPUT_TOPIC = "streams-chunked-output";
    private static final String SOURCE_NAME = "SOURCE";
    private static final String SINK_NAME = "SINK";


    @Bean
    public KafkaStreams configureKafka() {

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> lines = builder.stream(inputTopic);

        lines.foreach((key, value) -> logger.info("Value:" + value));

        //KafkaStreams streams = new KafkaStreams(builder.build(), getStreamConfiguration());
        KafkaStreams streams = new KafkaStreams(getTopology(), getStreamConfiguration());
        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        return streams;
    }

    private Properties getStreamConfiguration() {
        final Properties streamsConfiguration = new Properties();

        //ensure the stream always start at the beginning
        streamsConfiguration.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, consumerAutoOffsetReset);
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());

        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "kafka-stream-store/" + UUID.randomUUID().toString());
        //streamsConfiguration.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        return streamsConfiguration;
    }

    private Topology getTopology() {
        final Topology topology = new Topology();
        topology.addSource(SOURCE_NAME, new StringDeserializer(), new StringDeserializer(), inputTopic);
        topology.addProcessor(processorClassName, () -> new ClaimMessageProcessor(batchMessageCount, waitInterval), SOURCE_NAME);
        topology.addStateStore(getStoreBuilder());
        topology.connectProcessorAndStateStores(processorClassName, processorClassName);
        topology.addSink(SINK_NAME, OUTPUT_TOPIC, new StringSerializer(), new StringSerializer(), processorClassName);
        return topology;
    }


    private StoreBuilder<KeyValueStore<Long, String>> getStoreBuilder() {
        return Stores.keyValueStoreBuilder(
//                Stores.persistentKeyValueStore(processorClassName),
                Stores.inMemoryKeyValueStore(processorClassName),
                Serdes.Long(),
                Serdes.String());
    }
}
