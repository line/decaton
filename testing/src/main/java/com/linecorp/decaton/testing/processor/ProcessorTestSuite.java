package com.linecorp.decaton.testing.processor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;

import com.linecorp.decaton.client.DecatonClient;
import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.ProcessorsBuilder;
import com.linecorp.decaton.processor.PropertySupplier;
import com.linecorp.decaton.processor.runtime.ProcessorSubscription;
import com.linecorp.decaton.processor.runtime.RetryConfig;
import com.linecorp.decaton.protobuf.ProtocolBuffersDeserializer;
import com.linecorp.decaton.testing.KafkaClusterRule;
import com.linecorp.decaton.testing.TestUtils;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.Value;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/**
 * Checks basic functionality which Decaton processor should satisfy in whatever type of processing.
 *
 * Basic functionality here includes:
 * - All produced tasks are processed without leaking
 * - Preserve key based ordering
 * - Rebalance doesn't break above functionalities
 *
 * Note that this class holds entire produced tasks and processed tasks during test.
 * Supplying huge amount of tasks may pressure JVM heap quite a lot.
 */
@Slf4j
@RequiredArgsConstructor
public class ProcessorTestSuite<T extends MessageLite> {
    private final KafkaClusterRule rule;
    private final Parser<T> parser;
    private final int numSubscriptionInstance;
    private final int numTasks;
    private final Iterator<DecatonProducerRecord<T>> tasks;
    private final Function<ProcessorsBuilder<T>, ProcessorsBuilder<T>> configureProcessorsBuilder;
    private final RetryConfig retryConfig;
    private final PropertySupplier propertySuppliers;
    private final ProcessOrdering processOrdering;

    private static final int NUM_PARTITIONS = 8;

    /**
     * Process ordering the test should satisfy
     */
    public enum ProcessOrdering {
        /**
         * All produced tasks are processed in order only once
         */
        STRICT,

        /**
         * All produced tasks are processed in order, but allowing duplication.
         * Example:
         * <pre>
         * {@code
         *   say produced offsets are: [1,2,3,4,5]
         *   valid processing order:
         *     - [1,2,3,4,5]
         *     - [1,2,1,2,3,4,5] => when processor restarted after processed 2 before committing offset 1
         *     - [1,2,3,4,5,5] => when processor restarted after processed 5 and committed 4 before committing offset 5
         *   invalid processing order:
         *     - [1,3,2,4,5] => gapping
         *     - [1,2,3,4,5,4,3] => reconsumed from 4 implies offset 3 is committed. 3 cannot appear after processing 4
         * }
         * </pre>
         */
        ALLOW_DUPLICATE;

        public <T> boolean inOrder(List<T> produced,
                                   List<T> processed) {
            switch (this) {
                case STRICT:
                    return produced.equals(processed);
                case ALLOW_DUPLICATE:
                    // trivial checking
                    if (produced.isEmpty() && processed.isEmpty()) {
                        return true;
                    }
                    if (produced.isEmpty()) {
                        return false;
                    }
                    if (produced.size() > processed.size()) {
                        return false;
                    }
                    Map<T, Integer> toIndex = new HashMap<>();
                    for (int i = 0; i < produced.size(); i++) {
                        toIndex.putIfAbsent(produced.get(i), i);
                    }

                    // offset in produced tasks which is currently being processed
                    // ("offset" here means "sequence in subpartition" rather than Kafka's offset)
                    int offset = -1;
                    for (int i = 0; i < processed.size(); i++) {
                        // if there is a room, advance the offset
                        if (offset < produced.size() - 1) {
                            offset++;
                        }

                        T x = produced.get(offset);
                        T y = processed.get(i);
                        if (!x.equals(y)) {
                            // if processing task doesn't match to produced task, it means re-consuming happens.
                            // lookup the offset to rewind
                            Integer lookup = toIndex.get(y);

                            // rewound offset cannot be greater than current offset
                            if (lookup == null || lookup > offset) {
                                return false;
                            }
                            offset = lookup;
                        }
                    }
                    // must be processed until the end of produced tasks
                    return offset == produced.size() - 1;
            }
            throw new RuntimeException("Unreachable");
        }
    }

    @Value
    @Accessors(fluent = true)
    public static class DecatonProducerRecord<T> {
        String key;
        T task;
    }

    @Setter
    @Accessors(fluent = true)
    public static class Builder<T extends MessageLite> {
        private final KafkaClusterRule rule;
        private final Parser<T> parser;

        /**
         * Number of subscription instances which consumes tasks
         */
        private int numSubscriptionInstances = 3;
        /**
         * Configure test-specific processing logic
         */
        private Function<ProcessorsBuilder<T>, ProcessorsBuilder<T>> configureProcessorsBuilder;
        private RetryConfig retryConfig;
        private PropertySupplier propertySupplier;
        /**
         * Expected process ordering for this test
         */
        private ProcessOrdering expectOrdering = ProcessOrdering.ALLOW_DUPLICATE;

        @Setter(AccessLevel.NONE)
        private int numTasks;
        @Setter(AccessLevel.NONE)
        private Iterator<DecatonProducerRecord<T>> taskIterator;

        private Builder(KafkaClusterRule rule, Parser<T> parser) {
            this.rule = rule;
            this.parser = parser;
        }

        /**
         * Configure tasks which are produced to the topic.
         * Note that all tasks should be differentiated from others (i.e. equals() never be true between tasks)
         * since message ordering will be verified by checking produced task equals to processed task one by one.
         *
         * @param numTasks number of tasks
         * @param taskIterator iterator which generates tasks
         */
        public Builder<T> produce(int numTasks,
                                  Iterator<DecatonProducerRecord<T>> taskIterator) {
            this.numTasks = numTasks;
            this.taskIterator = taskIterator;
            return this;
        }

        public ProcessorTestSuite<T> build() {
            return new ProcessorTestSuite<>(rule,
                                            parser,
                                            numSubscriptionInstances,
                                            numTasks,
                                            taskIterator,
                                            configureProcessorsBuilder,
                                            retryConfig,
                                            propertySupplier,
                                            expectOrdering);
        }
    }

    public static <T extends MessageLite> Builder<T> builder(KafkaClusterRule rule,
                                                             Parser<T> parser) {
        return new Builder<>(rule, parser);
    }

    /**
     * Checks processor's basic functionality.
     * scenario:
     *   1. Start multiple subscription instances
     *   2. Produce tasks
     *   3. When half of tasks are processed, restart subscriptions one by one
     *   4. Await all tasks be processed
     *   5. Check process ordering
     */
    public void run() {
        String topicName = rule.admin().createRandomTopic(NUM_PARTITIONS, 3);

        CountDownLatch processLatch = new CountDownLatch(numTasks);
        CountDownLatch rollingRestartLatch = new CountDownLatch(numTasks / 2);

        List<DecatonProducerRecord<T>> processedTasks = Collections.synchronizedList(new ArrayList<>());
        Set<T> processedTaskSet = Collections.synchronizedSet(new HashSet<>());

        DecatonProcessor<T> globalProcessor = (context, task) -> {
            // If a task is retried, ordering is no longer preserved
            if (context.metadata().retryCount() == 0) {
                rollingRestartLatch.countDown();
                processedTasks.add(new DecatonProducerRecord<>(context.key(), task));
                if (processedTaskSet.add(task)) {
                    processLatch.countDown();
                }
            }
            context.deferCompletion().completeWith(context.push(task));
        };

        Supplier<ProcessorSubscription> subscriptionSupplier = () -> {
            ProcessorsBuilder<T> processorsBuilder =
                    ProcessorsBuilder.consuming(topicName,
                                                new ProtocolBuffersDeserializer<>(parser))
                                     .thenProcess(globalProcessor);

            return TestUtils.subscription(rule.bootstrapServers(),
                                          configureProcessorsBuilder.apply(processorsBuilder),
                                          retryConfig,
                                          propertySuppliers);
        };

        DecatonClient<T> client = null;
        ProcessorSubscription[] subscriptions = new ProcessorSubscription[numSubscriptionInstance];
        try {
            client = TestUtils.client(topicName, rule.bootstrapServers());
            for (int i = 0; i < numSubscriptionInstance; i++) {
                subscriptions[i] = subscriptionSupplier.get();
            }

            Map<String, List<T>> producedTasks = new HashMap<>();
            while(tasks.hasNext()) {
                DecatonProducerRecord<T> record = tasks.next();
                client.put(record.key(), record.task());
                List<T> tasks = producedTasks.computeIfAbsent(record.key(), key -> new ArrayList<>());
                tasks.add(record.task());
            }

            rollingRestartLatch.await();
            for (int i = 0; i < numSubscriptionInstance; i++) {
                subscriptions[i].close();
                subscriptions[i] = subscriptionSupplier.get();
            }

            processLatch.await();
            for (int i = 0; i < numSubscriptionInstance; i++) {
                subscriptions[i].close();
            }

            Map<String, List<T>> processedTasksPerKey = new HashMap<>();
            for (DecatonProducerRecord<T> record : processedTasks) {
                processedTasksPerKey.computeIfAbsent(record.key(),
                                                     key -> new ArrayList<>()).add(record.task());
            }

            assertEquals("all produced keys must be processed",
                         producedTasks.size(), processedTasksPerKey.size());
            for (Entry<String, List<T>> entry : producedTasks.entrySet()) {
                List<T> produced = entry.getValue();
                List<T> processed = processedTasksPerKey.get(entry.getKey());

                assertTrue("all produced tasks must be processed in order per key",
                           processOrdering.inOrder(produced, processed));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            safeClose(client);
            for (ProcessorSubscription subscription : subscriptions) {
                safeClose(subscription);
            }
            rule.admin().deleteTopics(topicName);
        }
    }

    private static void safeClose(AutoCloseable resource) {
        try {
            if (resource != null) {
                resource.close();
            }
        } catch (Exception e) {
            log.warn("Failed to close the resource", e);
        }
    }
}
