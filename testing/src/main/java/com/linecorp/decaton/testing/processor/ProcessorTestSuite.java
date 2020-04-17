package com.linecorp.decaton.testing.processor;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;

import com.linecorp.decaton.client.DecatonClient;
import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.ProcessorProperties;
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
 * - All produced tasks are processed without leaking nor duplication
 *     * Decaton adopts at-least-once processing semantics which allows duplication in principle, but
 *       in ordinary situation (i.e. consumers and brokers are working fine and tasks are completed
 *       within {@link ProcessorProperties#CONFIG_GROUP_REBALANCE_TIMEOUT_MS} during rebalance),
 *       it's expected duplication not to occur
 * - Preserve key based ordering
 * - Rebalance doesn't break above functionalities
 *
 * Note that this class holds entire produced tasks and consumed tasks during test.
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
    private final PropertySupplier propertySupplier;

    private static final int NUM_PARTITIONS = 8;

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

        @Setter(AccessLevel.NONE)
        private int numTasks;
        @Setter(AccessLevel.NONE)
        private Iterator<DecatonProducerRecord<T>> taskIterator;

        private Builder(KafkaClusterRule rule, Parser<T> parser) {
            this.rule = rule;
            this.parser = parser;
        }

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
                                            propertySupplier);
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
     *   5. Compare processed tasks to produced tasks
     */
    public void run() {
        String topicName = rule.admin().createRandomTopic(NUM_PARTITIONS, 3);

        CountDownLatch processLatch = new CountDownLatch(numTasks);
        CountDownLatch rollingRestartLatch = new CountDownLatch(numTasks / 2);

        List<DecatonProducerRecord<T>> processedTasks = Collections.synchronizedList(new ArrayList<>());
        DecatonProcessor<T> globalProcessor = (context, task) -> {
            // If a task is retried, ordering is no longer preserved
            if (context.metadata().retryCount() == 0) {
                processedTasks.add(new DecatonProducerRecord<>(context.key(), task));
                processLatch.countDown();
                rollingRestartLatch.countDown();
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
                                          propertySupplier);
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

            // Using assertTrue over assertEquals not to explode log when test fails
            assertTrue("all produced keys must be processed",
                       producedTasks.keySet().equals(processedTasksPerKey.keySet()));

            for (String key : producedTasks.keySet()) {
                List<T> produced = producedTasks.get(key);
                List<T> processed = processedTasksPerKey.get(key);

                assertTrue("all produced tasks must be processed in order per key without duplication",
                           produced.equals(processed));
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
