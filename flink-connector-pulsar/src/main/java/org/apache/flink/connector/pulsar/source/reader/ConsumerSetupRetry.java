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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.ConsumerBusyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;

/**
 * Retries the Pulsar calls that set up a split's consumer, with exponential backoff and jitter.
 *
 * <p>On restore, a split reader seeks the subscription with a temporary consumer and then creates
 * the real consumer, both on the same exclusive subscription. With thousands of splits starting at
 * once, a few of these calls fail transiently: the broker hasn't released the previous consumer
 * yet ({@link ConsumerBusyException}), or the client closed a connection that other splits share
 * because a consumer was closed while it was reconnecting. A single failed split fails the task
 * and restarts the job, so these calls retry instead.
 *
 * <p>Only errors the Pulsar client treats as retriable are retried, plus {@link
 * ConsumerBusyException}. Pulsar treats a busy consumer as permanent, but here it means the broker
 * still holds the consumer that was just closed, which clears within seconds.
 */
@Internal
final class ConsumerSetupRetry {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerSetupRetry.class);

    static final int DEFAULT_MAX_RETRIES = 6;
    static final long DEFAULT_INITIAL_BACKOFF_MS = 1_000;
    static final long DEFAULT_MAX_BACKOFF_MS = 30_000;

    /** A Pulsar call to retry. */
    @FunctionalInterface
    interface PulsarCall<T> {
        T call() throws PulsarClientException;
    }

    /** Waits between attempts. */
    @FunctionalInterface
    interface Sleeper {
        void sleep(long millis) throws InterruptedException;
    }

    private final int maxRetries;
    private final long initialBackoffMs;
    private final long maxBackoffMs;
    private final DoubleSupplier jitter;
    private final Sleeper sleeper;

    @VisibleForTesting
    ConsumerSetupRetry(
            int maxRetries,
            long initialBackoffMs,
            long maxBackoffMs,
            DoubleSupplier jitter,
            Sleeper sleeper) {
        this.maxRetries = maxRetries;
        this.initialBackoffMs = initialBackoffMs;
        this.maxBackoffMs = maxBackoffMs;
        this.jitter = jitter;
        this.sleeper = sleeper;
    }

    /** About 6 retries, backing off from 1 s to 30 s with up to 50% jitter either way. */
    static ConsumerSetupRetry withDefaults() {
        return new ConsumerSetupRetry(
                DEFAULT_MAX_RETRIES,
                DEFAULT_INITIAL_BACKOFF_MS,
                DEFAULT_MAX_BACKOFF_MS,
                () -> ThreadLocalRandom.current().nextDouble(),
                Thread::sleep);
    }

    /**
     * Runs the call, retrying retriable failures. Throws the last failure once retries run out, or
     * the first failure that isn't retriable.
     */
    <T> T call(String action, TopicPartition partition, PulsarCall<T> call)
            throws PulsarClientException, InterruptedException {
        int retry = 0;
        while (true) {
            try {
                return call.call();
            } catch (PulsarClientException e) {
                if (retry >= maxRetries || !isRetriable(e)) {
                    throw e;
                }
                long backoffMs = backoffMs(retry);
                retry++;
                LOG.warn(
                        "Failed to {} on partition {} (retry {} of {}), retrying in {} ms",
                        action,
                        partition,
                        retry,
                        maxRetries,
                        backoffMs,
                        e);
                sleeper.sleep(backoffMs);
            }
        }
    }

    @VisibleForTesting
    static boolean isRetriable(PulsarClientException e) {
        return e instanceof ConsumerBusyException || PulsarClientException.isRetriableError(e);
    }

    /** Exponential backoff for the given zero-based retry, jittered by up to 50% and capped. */
    @VisibleForTesting
    long backoffMs(int retry) {
        long base = initialBackoffMs << Math.min(retry, 30);
        if (base <= 0 || base > maxBackoffMs) {
            base = maxBackoffMs;
        }
        long jittered = (long) (base * (0.5 + jitter.getAsDouble()));
        return Math.min(jittered, maxBackoffMs);
    }
}
