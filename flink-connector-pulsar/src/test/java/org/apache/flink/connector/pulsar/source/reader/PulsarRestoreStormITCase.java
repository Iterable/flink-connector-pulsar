/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.pulsar.source.reader;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.pulsar.common.crypto.PulsarCrypto;
import org.apache.flink.connector.pulsar.common.schema.BytesSchema;
import org.apache.flink.connector.pulsar.common.schema.PulsarSchema;
import org.apache.flink.connector.pulsar.source.config.SourceConfiguration;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;
import org.apache.flink.connector.pulsar.testutils.PulsarTestSuiteBase;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.singletonList;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_SUBSCRIPTION_NAME;
import static org.apache.flink.metrics.groups.UnregisteredMetricsGroup.createSourceReaderMetricGroup;
import static org.apache.pulsar.client.api.Schema.STRING;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reproduces the restore storm from a job that reads thousands of partitions on one exclusive
 * subscription.
 *
 * <p>Every restored split seeks the subscription with a temporary consumer and then subscribes the
 * real one, and all splits of a subtask share one client. This test starts a consumer per
 * partition, drops the clients the way a lost TaskManager does, and restores every split at once,
 * several times. Any split that fails to set up would restart the whole job.
 *
 * <p>It's heavy, so it only runs when {@code pulsar.restore.repro.partitions} is set, for example
 * {@code -Dpulsar.restore.repro.partitions=2000}.
 */
@EnabledIfSystemProperty(named = "pulsar.restore.repro.partitions", matches = "\\d+")
class PulsarRestoreStormITCase extends PulsarTestSuiteBase {

    private static final int SUBTASKS = intProperty("pulsar.restore.repro.subtasks", 16);
    private static final int ROUNDS = intProperty("pulsar.restore.repro.rounds", 3);
    private static final int SETUP_THREADS = 256;

    @Test
    void restoredSplitsAllSetUpAfterClientsDrop() throws Exception {
        int partitions = intProperty("pulsar.restore.repro.partitions", 2000);
        String topic = "restore-storm-" + randomAlphabetic(8);
        operator().createTopic(topic, partitions);
        SourceConfiguration config = sourceConfig("restore-storm-" + randomAlphabetic(8));

        List<TopicPartition> topicPartitions = new ArrayList<>();
        for (int i = 0; i < partitions; i++) {
            topicPartitions.add(new TopicPartition(topic, i));
        }

        List<PulsarClient> clients = newClients();
        Map<String, AtomicInteger> startFailures =
                setUpSplits(clients, config, topicPartitions, null);
        assertThat(startFailures).as("failed splits on first start").isEmpty();

        List<String> roundFailures = new ArrayList<>();
        for (int round = 1; round <= ROUNDS; round++) {
            for (PulsarClient client : clients) {
                client.shutdown();
            }
            clients = newClients();
            Map<String, AtomicInteger> failures =
                    setUpSplits(clients, config, topicPartitions, MessageId.earliest);
            System.out.printf(
                    "restore round %d of %d, %d partitions: %s%n",
                    round, ROUNDS, partitions, failures.isEmpty() ? "no failed splits" : failures);
            if (!failures.isEmpty()) {
                roundFailures.add("round " + round + ": " + failures);
            }
        }
        for (PulsarClient client : clients) {
            client.shutdown();
        }

        assertThat(roundFailures).as("failed splits on restore").isEmpty();
    }

    /** Sets up one split reader per partition concurrently, like one fetcher thread per split. */
    private Map<String, AtomicInteger> setUpSplits(
            List<PulsarClient> clients,
            SourceConfiguration config,
            List<TopicPartition> topicPartitions,
            MessageId latestConsumedId)
            throws Exception {
        Map<String, AtomicInteger> failures = new ConcurrentHashMap<>();
        ExecutorService executor = Executors.newFixedThreadPool(SETUP_THREADS);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < topicPartitions.size(); i++) {
                PulsarClient client = clients.get(i % clients.size());
                TopicPartition partition = topicPartitions.get(i);
                futures.add(
                        executor.submit(
                                () -> {
                                    try {
                                        setUpSplit(client, config, partition, latestConsumedId);
                                    } catch (RuntimeException e) {
                                        failures.computeIfAbsent(
                                                        rootCauseName(e), k -> new AtomicInteger())
                                                .incrementAndGet();
                                    }
                                }));
            }
            for (Future<?> future : futures) {
                future.get();
            }
        } finally {
            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.MINUTES);
        }
        return failures;
    }

    private void setUpSplit(
            PulsarClient client,
            SourceConfiguration config,
            TopicPartition partition,
            MessageId latestConsumedId) {
        PulsarPartitionSplitReader reader =
                new PulsarPartitionSplitReader(
                        client,
                        config,
                        new BytesSchema(new PulsarSchema<>(STRING)),
                        PulsarCrypto.disabled(),
                        createSourceReaderMetricGroup());
        PulsarPartitionSplit split =
                new PulsarPartitionSplit(partition, StopCursor.never(), latestConsumedId, null);
        reader.handleSplitsChanges(new SplitsAddition<>(singletonList(split)));
    }

    /** Clients shaped like a production subtask's: one per subtask, four connections a broker. */
    private List<PulsarClient> newClients() throws PulsarClientException {
        List<PulsarClient> clients = new ArrayList<>();
        for (int i = 0; i < SUBTASKS; i++) {
            clients.add(
                    PulsarClient.builder()
                            .serviceUrl(operator().serviceUrl())
                            .connectionsPerBroker(4)
                            .ioThreads(4)
                            .listenerThreads(4)
                            .build());
        }
        return clients;
    }

    private SourceConfiguration sourceConfig(String subscriptionName) {
        Configuration config = operator().config();
        config.set(PULSAR_SUBSCRIPTION_NAME, subscriptionName);
        return new SourceConfiguration(config);
    }

    private static int intProperty(String name, int defaultValue) {
        String value = System.getProperties().getProperty(name);
        return value == null ? defaultValue : Integer.parseInt(value);
    }

    private static String rootCauseName(Throwable e) {
        Throwable cause = e;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        return cause.getClass().getSimpleName();
    }
}
