package gov.va.vba.kafka.streams.spring.demo.processors;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class ClaimMessageProcessor implements Processor<Long, String> {

    private int batchSize;
    private int waitTime;

    private KeyValueStore<Long, String> keyValueStore;
    private ProcessorContext processorContext = null;
    private final ObjectMapper mapper = new ObjectMapper();
    private final AtomicLong aggregatedClaimCount = new AtomicLong(0L);
    private final AtomicLong messageCount = new AtomicLong(0L);

    public ClaimMessageProcessor(int batchSize, int waitTime) {
        this.batchSize = batchSize;
        this.waitTime = waitTime;
    }

    private Logger logger = LoggerFactory.getLogger(ClaimMessageProcessor.class);

    @Override
    @SuppressWarnings("unchecked") // need specific type of state store
    public void init(final ProcessorContext processorContext) {
        this.processorContext = processorContext;
        keyValueStore = (KeyValueStore<Long, String>) processorContext.getStateStore(ClaimMessageProcessor.class.getName());
        processorContext.schedule(
                Duration.ofMillis(waitTime), PunctuationType.WALL_CLOCK_TIME, getPunctuator());
    }

    /**
     * This is the method that handles the state store after {@value {spring.kafka.consumer.wait-interval-ms}} seconds.  The goal is to commit
     * {@value {spring.kafka.consumer.batch-message-count}} values as a list in JSON format and delete those values from the store.
     *
     * @return A {@link Punctuator}.
     */
    private Punctuator getPunctuator() {
        return (timestamp) -> {
            logger.info("Processing key/value pairs");
            handleKeyValues(getKeyValueData().iterator());
            keyValueStore.flush();
            processorContext.commit();
        };
    }

    /**
     * Schedules a delete from the store and adds the value to a list of strings to forward.  If the list is size
     * {@value {spring.kafka.consumer.batch-message-count}} or there's no more records in the store, then forward on a list as a JSON string
     * of the values.
     *
     * @param iterator The {@link KeyValueIterator} we're getting from the store.
     */
    private void handleKeyValues(final Iterator<KeyValue<Long, String>> iterator) {
        final List<String> strings = new ArrayList<>();
        while (iterator.hasNext()) {
            final KeyValue<Long, String> keyValue = iterator.next();
            keyValueStore.delete(keyValue.key);
            strings.add(keyValue.value);
            if (strings.size() == batchSize || !iterator.hasNext()) {
                processTopicMessagesInBatch(strings);
                strings.clear();
            }
        }
    }

    private void processTopicMessagesInBatch(List<String> messages) {
        logger.info("got:" + messages.size() + " claims from topic");
        for(String message : messages) {
            logger.info(message);
            processorContext.forward(Long.toString(aggregatedClaimCount.incrementAndGet()), message);
        }
    }

    /**
     * Gets the data from the store, then sorts it based on timestamp.
     *
     * @return a list of key values sorted by timestamp
     */
    private List<KeyValue<Long, String>> getKeyValueData() {
        final KeyValueIterator<Long, String> iterator = keyValueStore.all();
        final List<KeyValue<Long, String>> keyValues = new ArrayList<>();
        while (iterator.hasNext()) {
            keyValues.add(iterator.next());
        }
        iterator.close();
        return keyValues.stream()
                .sorted(Comparator.comparingLong(e -> e.key))
                .collect(Collectors.toList());
    }

    /**
     * Converts a list of strings to JSON.
     *
     * @param strings The strings.
     * @return The JSON string.
     */
    private String getJsonData(final List<String> strings) {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            mapper.writeValue(out, strings);
        } catch (final IOException e) {
            throw new RuntimeException("error trying to parse json!", e);
        }
        return new String(out.toByteArray());
    }

    /**
     * Breaks line of text into words, then gives a key value for each word.
     * @param key for the record, but not used in this context
     * @param line the line of text
     */
    @Override
    public void process(final Long key, final String line) {
        keyValueStore.put(messageCount.incrementAndGet(), line);
    }

    @Override
    public void close() {
        //nothing to do
    }
}
