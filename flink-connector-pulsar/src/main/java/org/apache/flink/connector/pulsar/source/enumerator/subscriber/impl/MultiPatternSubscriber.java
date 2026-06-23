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

import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.enumerator.topic.range.RangeGenerator;

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.client.impl.LookupService;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace.Mode;
import org.apache.pulsar.common.lookup.GetTopicsResult;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils.isInternal;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Subscribe to matching topics based on multiple topic patterns. */
public class MultiPatternSubscriber extends BasePulsarSubscriber {
    private static final long serialVersionUID = 1L;

    private final List<PatternInfo> patterns;
    private final Mode subscriptionMode;

    public MultiPatternSubscriber(
            List<Pattern> topicPatterns, RegexSubscriptionMode subscriptionMode) {
        checkNotNull(topicPatterns, "Topic patterns cannot be null");
        checkArgument(!topicPatterns.isEmpty(), "Topic patterns cannot be empty");

        this.subscriptionMode = convertRegexSubscriptionMode(subscriptionMode);
        this.patterns = new ArrayList<>(topicPatterns.size());

        for (Pattern topicPattern : topicPatterns) {
            TopicName destination = TopicName.get(topicPattern.pattern());
            String pattern = destination.toString();

            Pattern shortenedPattern = Pattern.compile(pattern.split("://")[1]);
            String namespace = destination.getNamespaceObject().toString();

            this.patterns.add(new PatternInfo(shortenedPattern, namespace));
        }
    }

    @Override
    public Set<TopicPartition> getSubscribedTopicPartitions(
            RangeGenerator generator, int parallelism) throws Exception {
        Set<String> allTopics = new HashSet<>();

        for (PatternInfo patternInfo : patterns) {
            Set<String> topics = queryTopicsForPattern(patternInfo);
            allTopics.addAll(topics);
        }

        return createTopicPartitions(allTopics, generator, parallelism);
    }

    /**
     * Query topics for a single pattern using Pulsar's internal protocol. This method is similar to
     * TopicPatternSubscriber's implementation.
     */
    private Set<String> queryTopicsForPattern(PatternInfo patternInfo)
            throws PulsarClientException {
        checkNotNull(client, "This subscriber doesn't initialize properly.");

        LookupService lookupService = ((PulsarClientImpl) client).getLookup();
        NamespaceName namespaceName = NamespaceName.get(patternInfo.namespace);

        try {
            // Pulsar 2.11.0 can filter regular expression on broker, but it has a bug which can
            // only be used for wildcard filtering.
            String queryPattern = patternInfo.shortenedPattern.toString();
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
                        && (topicsResult.isFiltered()
                                || matchesTopicPattern(topic, patternInfo.shortenedPattern))) {
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
     * Check if the topic matches the given pattern. This method is copied from {@link
     * org.apache.pulsar.common.topics.TopicList#filterTopics(List, String)}.
     */
    private boolean matchesTopicPattern(String topic, Pattern pattern) {
        String shortenedTopic = topic.split("://")[1];
        return pattern.matcher(shortenedTopic).matches();
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

    /** Internal class to hold pattern information. */
    private static class PatternInfo implements Serializable {
        private static final long serialVersionUID = 1L;

        private final Pattern shortenedPattern;
        private final String namespace;

        PatternInfo(Pattern shortenedPattern, String namespace) {
            this.shortenedPattern = shortenedPattern;
            this.namespace = namespace;
        }
    }
}
