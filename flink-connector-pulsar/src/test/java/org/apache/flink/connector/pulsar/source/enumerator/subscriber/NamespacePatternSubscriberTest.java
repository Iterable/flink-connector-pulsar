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
import org.apache.flink.connector.pulsar.source.enumerator.subscriber.impl.NamespacePatternSubscriber;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.enumerator.topic.range.FullRangeGenerator;
import org.apache.flink.connector.pulsar.testutils.PulsarTestSuiteBase;
import org.apache.flink.util.InstantiationUtil;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.apache.flink.connector.pulsar.source.enumerator.subscriber.PulsarSubscriber.getNamespacePatternSubscriber;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils.topicName;
import static org.apache.pulsar.client.api.RegexSubscriptionMode.AllTopics;
import static org.apache.pulsar.client.api.RegexSubscriptionMode.PersistentOnly;
import static org.apache.pulsar.common.partition.PartitionedTopicMetadata.NON_PARTITIONED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link
 * org.apache.flink.connector.pulsar.source.enumerator.subscriber.impl.NamespacePatternSubscriber}.
 */
class NamespacePatternSubscriberTest extends PulsarTestSuiteBase {

    private static final int NUM_PARTITIONS_PER_TOPIC = 5;
    private static final int NUM_PARALLELISM = 10;

    // Namespace pattern: flink/ns-pattern-.*
    private final String ns1Topic1 = topicName("flink/ns-pattern-1/topic-" + randomAlphanumeric(4));
    private final String ns1Topic2 =
            topicName("flink/ns-pattern-1/service-" + randomAlphanumeric(4));
    private final String ns2Topic1 = topicName("flink/ns-pattern-2/topic-" + randomAlphanumeric(4));
    private final String ns2Topic2 =
            topicName("flink/ns-pattern-2/service-" + randomAlphanumeric(4));
    private final String ns3Topic1 =
            topicName("flink/ns-pattern-other/topic-" + randomAlphanumeric(4));

    // Non-partitioned topics
    private final String ns4Topic1 = topicName("flink/ns-pattern-3/event-" + randomAlphanumeric(4));

    @BeforeAll
    void setUp() throws Exception {
        operator().createNamespace("flink/ns-pattern-1");
        operator().createNamespace("flink/ns-pattern-2");
        operator().createNamespace("flink/ns-pattern-other");
        operator().createNamespace("flink/ns-pattern-3");

        // Create partitioned topics
        operator().createTopic(ns1Topic1, NUM_PARTITIONS_PER_TOPIC);
        operator().createTopic(ns1Topic2, NUM_PARTITIONS_PER_TOPIC);
        operator().createTopic(ns2Topic1, NUM_PARTITIONS_PER_TOPIC);
        operator().createTopic(ns2Topic2, NUM_PARTITIONS_PER_TOPIC);
        operator().createTopic(ns3Topic1, NUM_PARTITIONS_PER_TOPIC);

        // Create non-partitioned topics
        operator().createTopic(ns4Topic1, NON_PARTITIONED);
    }

    @AfterAll
    void tearDown() throws Exception {
        operator().deleteTopic(ns1Topic1);
        operator().deleteTopic(ns1Topic2);
        operator().deleteTopic(ns2Topic1);
        operator().deleteTopic(ns2Topic2);
        operator().deleteTopic(ns3Topic1);
        operator().deleteTopic(ns4Topic1);
    }

    // ==================== Pattern Validation Tests ====================

    @Test
    void invalidPatternWithOnePartThrowsException() {
        assertThatThrownBy(
                        () -> getNamespacePatternSubscriber(Pattern.compile("invalid"), AllTopics))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Pattern must be in format tenant/namespace-pattern/topic-pattern");
    }

    @Test
    void invalidPatternWithTwoPartsThrowsException() {
        assertThatThrownBy(
                        () ->
                                getNamespacePatternSubscriber(
                                        Pattern.compile("tenant/namespace"), AllTopics))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Pattern must be in format tenant/namespace-pattern/topic-pattern");
    }

    @Test
    void validPatternWithThreeParts() {
        PulsarSubscriber subscriber =
                getNamespacePatternSubscriber(
                        Pattern.compile("tenant/namespace-.*/topic-.*"), AllTopics);
        assertThat(subscriber).isNotNull();
    }

    @Test
    void validPatternWithProtocolPrefix() {
        PulsarSubscriber subscriber =
                getNamespacePatternSubscriber(
                        Pattern.compile("persistent://tenant/namespace-.*/topic-.*"), AllTopics);
        assertThat(subscriber).isNotNull();
    }

    @Test
    void validPatternWithComplexRegex() {
        PulsarSubscriber subscriber =
                getNamespacePatternSubscriber(
                        Pattern.compile("tenant/org-[0-9]+/[a-z]+-topic-[0-9]+"), AllTopics);
        assertThat(subscriber).isNotNull();
    }

    @Test
    void serializationRoundTrip() {
        PulsarSubscriber subscriber =
                getNamespacePatternSubscriber(
                        Pattern.compile("flink/ns-pattern-.*/topic-.*"), AllTopics);
        assertThatCode(() -> InstantiationUtil.clone(subscriber)).doesNotThrowAnyException();
    }

    // ==================== Admin URL Derivation Tests ====================

    @Test
    void deriveAdminUrlFromPlainServiceUrl() throws Exception {
        // Test that service URL pulsar://localhost:6650 derives to http://localhost:8080
        String derived = callDeriveAdminUrl("pulsar://localhost:6650");
        assertThat(derived).isEqualTo("http://localhost:8080");
    }

    @Test
    void deriveAdminUrlFromSslServiceUrl() throws Exception {
        // Test that service URL pulsar+ssl://localhost:6651 derives to https://localhost:8443
        String derived = callDeriveAdminUrl("pulsar+ssl://localhost:6651");
        assertThat(derived).isEqualTo("https://localhost:8443");
    }

    @Test
    void deriveAdminUrlFromServiceUrlWithoutPort() throws Exception {
        String derived = callDeriveAdminUrl("pulsar://localhost");
        assertThat(derived).isEqualTo("http://localhost:8080");
    }

    @Test
    void deriveAdminUrlFromServiceUrlWithMultipleBrokers() throws Exception {
        // Should take first broker only
        String derived = callDeriveAdminUrl("pulsar://broker1:6650,broker2:6650,broker3:6650");
        assertThat(derived).isEqualTo("http://broker1:8080");
    }

    @Test
    void deriveAdminUrlWithCustomHost() throws Exception {
        String derived = callDeriveAdminUrl("pulsar://my-cluster.example.com:6650");
        assertThat(derived).isEqualTo("http://my-cluster.example.com:8080");
    }

    // ==================== Integration Tests ====================

    @Test
    void discoverNamespacesAndTopicsAcrossMultipleNamespaces() throws Exception {
        // Pattern: flink/ns-pattern-.*/topic-.*
        TestableNamespacePatternSubscriber subscriber =
                new TestableNamespacePatternSubscriber(
                        Pattern.compile("flink/ns-pattern-.*/topic-.*"),
                        AllTopics,
                        operator().adminUrl());
        subscriber.open(operator().client());

        Set<TopicPartition> topicPartitions =
                subscriber.getSubscribedTopicPartitions(new FullRangeGenerator(), NUM_PARALLELISM);

        Set<TopicPartition> expectedPartitions = new HashSet<>();
        // Topics from ns-pattern-1, ns-pattern-2, and ns-pattern-other that match "topic-.*"
        for (int i = 0; i < NUM_PARTITIONS_PER_TOPIC; i++) {
            expectedPartitions.add(new TopicPartition(ns1Topic1, i));
            expectedPartitions.add(new TopicPartition(ns2Topic1, i));
            expectedPartitions.add(new TopicPartition(ns3Topic1, i));
        }

        assertThat(topicPartitions).isEqualTo(expectedPartitions);
    }

    @Test
    void discoverWithSpecificNamespacePattern() throws Exception {
        // Pattern: flink/ns-pattern-[12]/service-.*
        // Should only match ns-pattern-1 and ns-pattern-2, and only service-* topics
        TestableNamespacePatternSubscriber subscriber =
                new TestableNamespacePatternSubscriber(
                        Pattern.compile("flink/ns-pattern-[12]/service-.*"),
                        AllTopics,
                        operator().adminUrl());
        subscriber.open(operator().client());

        Set<TopicPartition> topicPartitions =
                subscriber.getSubscribedTopicPartitions(new FullRangeGenerator(), NUM_PARALLELISM);

        Set<TopicPartition> expectedPartitions = new HashSet<>();
        for (int i = 0; i < NUM_PARTITIONS_PER_TOPIC; i++) {
            expectedPartitions.add(new TopicPartition(ns1Topic2, i));
            expectedPartitions.add(new TopicPartition(ns2Topic2, i));
        }

        assertThat(topicPartitions).isEqualTo(expectedPartitions);
    }

    @Test
    void discoverNonPartitionedTopics() throws Exception {
        // Pattern: flink/ns-pattern-3/event-.*
        TestableNamespacePatternSubscriber subscriber =
                new TestableNamespacePatternSubscriber(
                        Pattern.compile("flink/ns-pattern-3/event-.*"),
                        AllTopics,
                        operator().adminUrl());
        subscriber.open(operator().client());

        Set<TopicPartition> topicPartitions =
                subscriber.getSubscribedTopicPartitions(new FullRangeGenerator(), NUM_PARALLELISM);

        Set<TopicPartition> expectedPartitions = new HashSet<>();
        expectedPartitions.add(new TopicPartition(ns4Topic1, -1));

        assertThat(topicPartitions).isEqualTo(expectedPartitions);
    }

    @Test
    void discoverWithPersistentOnlyMode() throws Exception {
        TestableNamespacePatternSubscriber subscriber =
                new TestableNamespacePatternSubscriber(
                        Pattern.compile("flink/ns-pattern-.*/topic-.*"),
                        PersistentOnly,
                        operator().adminUrl());
        subscriber.open(operator().client());

        Set<TopicPartition> topicPartitions =
                subscriber.getSubscribedTopicPartitions(new FullRangeGenerator(), NUM_PARALLELISM);

        // Should still find all topics since all are persistent
        Set<TopicPartition> expectedPartitions = new HashSet<>();
        for (int i = 0; i < NUM_PARTITIONS_PER_TOPIC; i++) {
            expectedPartitions.add(new TopicPartition(ns1Topic1, i));
            expectedPartitions.add(new TopicPartition(ns2Topic1, i));
            expectedPartitions.add(new TopicPartition(ns3Topic1, i));
        }

        assertThat(topicPartitions).isEqualTo(expectedPartitions);
    }

    @Test
    void discoverMixedPartitionedAndNonPartitioned() throws Exception {
        TestableNamespacePatternSubscriber subscriber =
                new TestableNamespacePatternSubscriber(
                        Pattern.compile("flink/ns-pattern-[13]/.*"),
                        AllTopics,
                        operator().adminUrl());
        subscriber.open(operator().client());

        Set<TopicPartition> topicPartitions =
                subscriber.getSubscribedTopicPartitions(new FullRangeGenerator(), NUM_PARALLELISM);

        Set<TopicPartition> expectedPartitions = new HashSet<>();
        // From ns-pattern-1 (partitioned)
        for (int i = 0; i < NUM_PARTITIONS_PER_TOPIC; i++) {
            expectedPartitions.add(new TopicPartition(ns1Topic1, i));
            expectedPartitions.add(new TopicPartition(ns1Topic2, i));
        }
        // From ns-pattern-3 (non-partitioned)
        expectedPartitions.add(new TopicPartition(ns4Topic1, -1));

        assertThat(topicPartitions).isEqualTo(expectedPartitions);
    }

    @Test
    void builderIntegrationTest() throws Exception {
        // Test that PulsarSourceBuilder properly integrates with NamespacePatternSubscriber
        PulsarSource<String> source =
                PulsarSource.builder()
                        .setServiceUrl(operator().serviceUrl())
                        .setSubscriptionName("test-namespace-pattern-subscription")
                        .setNamespaceTopicPattern(Pattern.compile("flink/ns-pattern-.*/topic-.*"))
                        .setDeserializationSchema(new SimpleStringSchema())
                        .build();

        assertThat(source).isNotNull();
        assertThat(source.getBoundedness()).isNotNull();
    }

    // ==================== Helper Methods ====================

    /**
     * Helper method to test deriveAdminUrl logic. Since deriveAdminUrl is now in
     * PulsarClientFactory, we test it indirectly through PulsarClientFactory.deriveAdminUrl via
     * reflection.
     */
    private String callDeriveAdminUrl(String serviceUrl) throws Exception {
        java.lang.reflect.Method method =
                org.apache.flink.connector.pulsar.common.config.PulsarClientFactory.class
                        .getDeclaredMethod("deriveAdminUrl", String.class);
        method.setAccessible(true);
        return (String) method.invoke(null, serviceUrl);
    }

    /**
     * Test-specific subclass that allows injecting the correct admin URL for Docker test
     * environment.
     */
    private static class TestableNamespacePatternSubscriber extends NamespacePatternSubscriber {
        private final String testAdminUrl;

        public TestableNamespacePatternSubscriber(
                Pattern fullPattern, RegexSubscriptionMode subscriptionMode, String adminUrl) {
            super(fullPattern, subscriptionMode);
            this.testAdminUrl = adminUrl;
        }

        @Override
        public void open(PulsarClient client) {
            // Call parent open to set client field
            super.open(client);

            // Create and inject admin with test URL
            try {
                if (client instanceof PulsarClientImpl) {
                    PulsarClientImpl clientImpl = (PulsarClientImpl) client;
                    ClientConfigurationData clientConfig = clientImpl.getConfiguration();
                    Authentication authentication = clientConfig.getAuthentication();

                    PulsarAdmin admin =
                            PulsarAdmin.builder()
                                    .serviceHttpUrl(testAdminUrl)
                                    .authentication(authentication)
                                    .build();

                    // Use the new setAdmin method
                    setAdmin(admin);
                }
            } catch (PulsarClientException e) {
                throw new RuntimeException("Failed to create test admin", e);
            }
        }
    }
}
