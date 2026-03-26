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

package org.apache.flink.connector.pulsar.source.enumerator.subscriber;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.enumerator.topic.range.FullRangeGenerator;
import org.apache.flink.connector.pulsar.testutils.PulsarTestSuiteBase;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.apache.flink.connector.pulsar.source.enumerator.subscriber.PulsarSubscriber.getMultiTopicPatternSubscriber;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils.topicName;
import static org.apache.pulsar.client.api.RegexSubscriptionMode.AllTopics;
import static org.apache.pulsar.client.api.RegexSubscriptionMode.PersistentOnly;
import static org.apache.pulsar.common.partition.PartitionedTopicMetadata.NON_PARTITIONED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link
 * org.apache.flink.connector.pulsar.source.enumerator.subscriber.impl.MultiPatternSubscriber}.
 */
class MultiPatternSubscriberTest extends PulsarTestSuiteBase {

    private static final int NUM_PARTITIONS_PER_TOPIC = 5;
    private static final int NUM_PARALLELISM = 10;

    // Namespace 1: flink/multi-pattern-1
    private final String ns1Topic1 =
            topicName("flink/multi-pattern-1/topic-alpha-" + randomAlphanumeric(4));
    private final String ns1Topic2 =
            topicName("flink/multi-pattern-1/topic-beta-" + randomAlphanumeric(4));
    private final String ns1Topic3 =
            topicName("flink/multi-pattern-1/topic-gamma-" + randomAlphanumeric(4));

    // Namespace 2: flink/multi-pattern-2
    private final String ns2Topic1 =
            topicName("flink/multi-pattern-2/service-alpha-" + randomAlphanumeric(4));
    private final String ns2Topic2 =
            topicName("flink/multi-pattern-2/service-beta-" + randomAlphanumeric(4));

    // Namespace 3: flink/multi-pattern-3 (non-partitioned)
    private final String ns3Topic1 =
            topicName("flink/multi-pattern-3/event-alpha-" + randomAlphanumeric(4));
    private final String ns3Topic2 =
            topicName("flink/multi-pattern-3/event-beta-" + randomAlphanumeric(4));

    @BeforeAll
    void setUp() throws Exception {
        operator().createNamespace("flink/multi-pattern-1");
        operator().createNamespace("flink/multi-pattern-2");
        operator().createNamespace("flink/multi-pattern-3");

        // Create partitioned topics
        operator().createTopic(ns1Topic1, NUM_PARTITIONS_PER_TOPIC);
        operator().createTopic(ns1Topic2, NUM_PARTITIONS_PER_TOPIC);
        operator().createTopic(ns1Topic3, NUM_PARTITIONS_PER_TOPIC);
        operator().createTopic(ns2Topic1, NUM_PARTITIONS_PER_TOPIC);
        operator().createTopic(ns2Topic2, NUM_PARTITIONS_PER_TOPIC);

        // Create non-partitioned topics
        operator().createTopic(ns3Topic1, NON_PARTITIONED);
        operator().createTopic(ns3Topic2, NON_PARTITIONED);
    }

    @AfterAll
    void tearDown() throws Exception {
        operator().deleteTopic(ns1Topic1);
        operator().deleteTopic(ns1Topic2);
        operator().deleteTopic(ns1Topic3);
        operator().deleteTopic(ns2Topic1);
        operator().deleteTopic(ns2Topic2);
        operator().deleteTopic(ns3Topic1);
        operator().deleteTopic(ns3Topic2);
    }

    @Test
    void multiplePatternsDifferentNamespaces() throws Exception {
        PulsarSubscriber subscriber =
                getMultiTopicPatternSubscriber(
                        Arrays.asList(
                                Pattern.compile("flink/multi-pattern-1/topic-alpha-.*"),
                                Pattern.compile("flink/multi-pattern-2/service-.*")),
                        AllTopics);
        subscriber.open(operator().client());

        Set<TopicPartition> topicPartitions =
                subscriber.getSubscribedTopicPartitions(new FullRangeGenerator(), NUM_PARALLELISM);

        Set<TopicPartition> expectedPartitions = new HashSet<>();
        for (int i = 0; i < NUM_PARTITIONS_PER_TOPIC; i++) {
            expectedPartitions.add(new TopicPartition(ns1Topic1, i));
            expectedPartitions.add(new TopicPartition(ns2Topic1, i));
            expectedPartitions.add(new TopicPartition(ns2Topic2, i));
        }

        assertThat(topicPartitions).isEqualTo(expectedPartitions);
    }

    @Test
    void multiplePatternsForSameNamespace() throws Exception {
        // Two patterns in the same namespace - should deduplicate overlapping topics
        PulsarSubscriber subscriber =
                getMultiTopicPatternSubscriber(
                        Arrays.asList(
                                Pattern.compile("flink/multi-pattern-1/topic-alpha-.*"),
                                Pattern.compile("flink/multi-pattern-1/topic-.*")),
                        AllTopics);
        subscriber.open(operator().client());

        Set<TopicPartition> topicPartitions =
                subscriber.getSubscribedTopicPartitions(new FullRangeGenerator(), NUM_PARALLELISM);

        Set<TopicPartition> expectedPartitions = new HashSet<>();
        // All three topics should be included, but no duplicates
        for (int i = 0; i < NUM_PARTITIONS_PER_TOPIC; i++) {
            expectedPartitions.add(new TopicPartition(ns1Topic1, i));
            expectedPartitions.add(new TopicPartition(ns1Topic2, i));
            expectedPartitions.add(new TopicPartition(ns1Topic3, i));
        }

        assertThat(topicPartitions).isEqualTo(expectedPartitions);
    }

    @Test
    void multiplePatternsDifferentSubscriptionModes() throws Exception {
        // Test with PersistentOnly mode
        PulsarSubscriber subscriber =
                getMultiTopicPatternSubscriber(
                        Arrays.asList(
                                Pattern.compile("flink/multi-pattern-1/topic-.*"),
                                Pattern.compile("flink/multi-pattern-2/service-.*")),
                        PersistentOnly);
        subscriber.open(operator().client());

        Set<TopicPartition> topicPartitions =
                subscriber.getSubscribedTopicPartitions(new FullRangeGenerator(), NUM_PARALLELISM);

        Set<TopicPartition> expectedPartitions = new HashSet<>();
        for (int i = 0; i < NUM_PARTITIONS_PER_TOPIC; i++) {
            expectedPartitions.add(new TopicPartition(ns1Topic1, i));
            expectedPartitions.add(new TopicPartition(ns1Topic2, i));
            expectedPartitions.add(new TopicPartition(ns1Topic3, i));
            expectedPartitions.add(new TopicPartition(ns2Topic1, i));
            expectedPartitions.add(new TopicPartition(ns2Topic2, i));
        }

        assertThat(topicPartitions).isEqualTo(expectedPartitions);
    }

    @Test
    void multiplePatternWithNonPartitionedTopics() throws Exception {
        PulsarSubscriber subscriber =
                getMultiTopicPatternSubscriber(
                        Arrays.asList(Pattern.compile("flink/multi-pattern-3/event-.*")),
                        AllTopics);
        subscriber.open(operator().client());

        Set<TopicPartition> topicPartitions =
                subscriber.getSubscribedTopicPartitions(new FullRangeGenerator(), NUM_PARALLELISM);

        Set<TopicPartition> expectedPartitions = new HashSet<>();
        expectedPartitions.add(new TopicPartition(ns3Topic1, -1));
        expectedPartitions.add(new TopicPartition(ns3Topic2, -1));

        assertThat(topicPartitions).isEqualTo(expectedPartitions);
    }

    @Test
    void multiplePatternsMixedPartitionedAndNonPartitioned() throws Exception {
        PulsarSubscriber subscriber =
                getMultiTopicPatternSubscriber(
                        Arrays.asList(
                                Pattern.compile("flink/multi-pattern-1/topic-alpha-.*"),
                                Pattern.compile("flink/multi-pattern-3/event-.*")),
                        AllTopics);
        subscriber.open(operator().client());

        Set<TopicPartition> topicPartitions =
                subscriber.getSubscribedTopicPartitions(new FullRangeGenerator(), NUM_PARALLELISM);

        Set<TopicPartition> expectedPartitions = new HashSet<>();
        for (int i = 0; i < NUM_PARTITIONS_PER_TOPIC; i++) {
            expectedPartitions.add(new TopicPartition(ns1Topic1, i));
        }
        expectedPartitions.add(new TopicPartition(ns3Topic1, -1));
        expectedPartitions.add(new TopicPartition(ns3Topic2, -1));

        assertThat(topicPartitions).isEqualTo(expectedPartitions);
    }

    @Test
    void emptyPatternListThrowsException() {
        assertThatThrownBy(() -> getMultiTopicPatternSubscriber(Arrays.asList(), AllTopics))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Topic patterns cannot be empty");
    }

    @Test
    void nullPatternListThrowsException() {
        assertThatThrownBy(() -> getMultiTopicPatternSubscriber(null, AllTopics))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Topic patterns cannot be null");
    }

    @Test
    void builderIntegrationTest() throws Exception {
        // Test that PulsarSourceBuilder properly integrates with MultiPatternSubscriber
        PulsarSource<String> source =
                PulsarSource.builder()
                        .setServiceUrl(operator().serviceUrl())
                        .setSubscriptionName("test-multi-pattern-subscription")
                        .setTopicPatterns(
                                Arrays.asList(
                                        Pattern.compile("flink/multi-pattern-1/topic-alpha-.*"),
                                        Pattern.compile("flink/multi-pattern-2/service-.*")))
                        .setDeserializationSchema(new SimpleStringSchema())
                        .build();

        assertThat(source).isNotNull();
        assertThat(source.getBoundedness()).isNotNull();
    }
}
