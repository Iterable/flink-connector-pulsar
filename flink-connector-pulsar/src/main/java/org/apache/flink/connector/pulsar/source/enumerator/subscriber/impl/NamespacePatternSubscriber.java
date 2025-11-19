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

package org.apache.flink.connector.pulsar.source.enumerator.subscriber.impl;

import org.apache.flink.connector.pulsar.source.enumerator.subscriber.RequiresPulsarAdmin;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.enumerator.topic.range.RangeGenerator;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.client.impl.LookupService;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace.Mode;
import org.apache.pulsar.common.lookup.GetTopicsResult;
import org.apache.pulsar.common.naming.NamespaceName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils.isInternal;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Subscribe to matching topics based on dynamic namespace and topic pattern discovery.
 *
 * <p>Pattern format: tenant/namespace-pattern/topic-pattern
 *
 * <p>Example: a pattern like eventbus/org-[0-9]+/topic-[a-z]+ will discover namespaces matching
 * org-[0-9]+ under tenant eventbus, then discover topics matching topic-[a-z]+ in each namespace.
 */
public class NamespacePatternSubscriber extends BasePulsarSubscriber
        implements RequiresPulsarAdmin {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(NamespacePatternSubscriber.class);

    private final String tenant;
    private final Pattern namespacePattern;
    private final Pattern topicPattern;
    private final Mode subscriptionMode;

    private transient PulsarAdmin admin;
    private transient List<String> lastKnownNamespaces;

    public NamespacePatternSubscriber(
            Pattern fullPattern, RegexSubscriptionMode subscriptionMode) {
        checkNotNull(fullPattern, "Full pattern cannot be null");

        String patternStr = fullPattern.pattern();
        // Remove protocol if present (e.g., persistent://)
        if (patternStr.contains("://")) {
            patternStr = patternStr.split("://")[1];
        }

        // Parse tenant/namespace-pattern/topic-pattern
        String[] parts = patternStr.split("/", 3);
        checkArgument(
                parts.length == 3,
                "Pattern must be in format tenant/namespace-pattern/topic-pattern, got: %s",
                patternStr);

        this.tenant = parts[0];
        this.namespacePattern = Pattern.compile(parts[1]);
        this.topicPattern = Pattern.compile(parts[2]);
        this.subscriptionMode = convertRegexSubscriptionMode(subscriptionMode);
        this.lastKnownNamespaces = new CopyOnWriteArrayList<>();

        LOG.info(
                "Created NamespacePatternSubscriber: tenant={}, namespacePattern={}, topicPattern={}",
                tenant,
                parts[1],
                parts[2]);
    }

    @Override
    public void setAdmin(PulsarAdmin admin) {
        this.admin = checkNotNull(admin, "PulsarAdmin cannot be null");
    }

    @Override
    public Set<TopicPartition> getSubscribedTopicPartitions(
            RangeGenerator generator, int parallelism) throws Exception {
        checkNotNull(client, "This subscriber doesn't initialize properly.");
        checkNotNull(admin, "PulsarAdmin not initialized properly.");

        // Step 1: Discover namespaces
        List<String> namespaces = discoverNamespaces();

        // Step 2: Discover topics for each namespace
        Set<String> allTopics = new HashSet<>();
        for (String namespace : namespaces) {
            Set<String> topics = queryTopicsForNamespace(namespace);
            allTopics.addAll(topics);
        }

        LOG.info(
                "Discovered {} topics across {} namespaces",
                allTopics.size(),
                namespaces.size());

        // Step 3: Convert to topic partitions
        return createTopicPartitions(allTopics, generator, parallelism);
    }

    /**
     * Discover namespaces matching the namespace pattern.
     *
     * <p>On success, updates lastKnownNamespaces. On failure, falls back to lastKnownNamespaces
     * and logs a warning.
     */
    private List<String> discoverNamespaces() {
        try {
            // Get all namespaces for the tenant
            List<String> allNamespaces = admin.namespaces().getNamespaces(tenant);

            // Filter by namespace pattern
            List<String> matchingNamespaces = new ArrayList<>();
            String expectedPrefix = tenant + "/";
            for (String namespace : allNamespaces) {
                // Validate namespace format before extracting local part
                if (!namespace.startsWith(expectedPrefix)) {
                    LOG.warn(
                            "Unexpected namespace format: {}, expected to start with {}",
                            namespace,
                            expectedPrefix);
                    continue;
                }
                // Extract local part (after tenant/)
                String localPart = namespace.substring(expectedPrefix.length());
                if (namespacePattern.matcher(localPart).matches()) {
                    matchingNamespaces.add(namespace);
                }
            }

            // Update last known namespaces
            lastKnownNamespaces = new CopyOnWriteArrayList<>(matchingNamespaces);

            LOG.debug(
                    "Discovered {} namespaces matching pattern {} under tenant {}",
                    matchingNamespaces.size(),
                    namespacePattern,
                    tenant);

            return matchingNamespaces;
        } catch (Exception e) {
            if (lastKnownNamespaces == null || lastKnownNamespaces.isEmpty()) {
                LOG.error(
                        "Failed to discover namespaces and no fallback available for tenant {}",
                        tenant,
                        e);
                throw new RuntimeException("Failed to discover namespaces", e);
            }

            LOG.warn(
                    "Failed to discover namespaces for tenant {}, using last known {} namespaces",
                    tenant,
                    lastKnownNamespaces.size(),
                    e);
            return lastKnownNamespaces;
        }
    }

    /**
     * Query topics for a single namespace using Pulsar's internal protocol.
     *
     * <p>Similar to TopicPatternSubscriber's implementation.
     */
    private Set<String> queryTopicsForNamespace(String namespace) throws PulsarClientException {
        LookupService lookupService = ((PulsarClientImpl) client).getLookup();
        NamespaceName namespaceName = NamespaceName.get(namespace);

        try {
            // Pulsar 2.11.0 can filter regular expression on broker, but it has a bug which can
            // only be used for wildcard filtering.
            // Construct full pattern: namespace/topic-pattern for broker-side filtering
            String fullPattern = namespace + "/" + topicPattern.toString();
            String queryPattern = fullPattern;
            if (!queryPattern.endsWith(".*")) {
                queryPattern = null;
            }

            GetTopicsResult topicsResult =
                    lookupService
                            .getTopicsUnderNamespace(
                                    namespaceName, subscriptionMode, queryPattern, null)
                            .get();

            List<String> topics = topicsResult.getTopics();
            Set<String> results = new HashSet<>(topics.size());

            // The regular expression filter may not be enabled in broker.
            // Add the filter here if the result is not filtered.
            for (String topic : topics) {
                if (!isInternal(topic)
                        && (topicsResult.isFiltered() || matchesTopicPattern(topic))) {
                    results.add(topic);
                }
            }

            return results;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException(e);
        } catch (ExecutionException e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    /**
     * Check if the topic matches the topic pattern.
     *
     * <p>This method is copied from {@link
     * org.apache.pulsar.common.topics.TopicList#filterTopics(List, String)}.
     */
    private boolean matchesTopicPattern(String topic) {
        // Extract topic name without protocol
        String[] parts = topic.split("://");
        if (parts.length < 2) {
            LOG.warn("Invalid topic format (missing protocol separator): {}", topic);
            return false;
        }
        String shortenedTopic = parts[1];
        // Extract topic name without namespace
        String topicName = shortenedTopic.substring(shortenedTopic.lastIndexOf('/') + 1);
        return topicPattern.matcher(topicName).matches();
    }

    /** Convert the subscription mode into the internal binary protocol. */
    private Mode convertRegexSubscriptionMode(RegexSubscriptionMode subscriptionMode) {
        switch (subscriptionMode) {
            case AllTopics:
                return Mode.ALL;
            case PersistentOnly:
                return Mode.PERSISTENT;
            case NonPersistentOnly:
                return Mode.NON_PERSISTENT;
            default:
                throw new IllegalArgumentException(
                        "We don't support such subscription mode " + subscriptionMode);
        }
    }
}
