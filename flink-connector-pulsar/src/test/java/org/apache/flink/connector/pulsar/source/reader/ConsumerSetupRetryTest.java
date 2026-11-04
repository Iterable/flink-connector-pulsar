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

import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.ConsumerBusyException;
import org.apache.pulsar.client.api.PulsarClientException.NotFoundException;
import org.apache.pulsar.client.api.PulsarClientException.TimeoutException;
import org.apache.pulsar.client.api.PulsarClientException.TopicDoesNotExistException;
import org.junit.jupiter.api.Test;

import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link ConsumerSetupRetry}. */
class ConsumerSetupRetryTest {

    private static final TopicPartition PARTITION = new TopicPartition("topic", 0);

    private final List<Long> sleeps = new ArrayList<>();

    @Test
    void returnsWithoutSleepingWhenTheFirstAttemptSucceeds() throws Exception {
        String result = retry(0.5).call("subscribe", PARTITION, () -> "consumer");

        assertThat(result).isEqualTo("consumer");
        assertThat(sleeps).isEmpty();
    }

    @Test
    void retriesTransientFailuresUntilACallSucceeds() throws Exception {
        AtomicInteger attempts = new AtomicInteger();

        String result =
                retry(0.5)
                        .call(
                                "subscribe",
                                PARTITION,
                                () -> {
                                    if (attempts.incrementAndGet() < 4) {
                                        throw closedChannel();
                                    }
                                    return "consumer";
                                });

        assertThat(result).isEqualTo("consumer");
        assertThat(attempts).hasValue(4);
        assertThat(sleeps).containsExactly(1_000L, 2_000L, 4_000L);
    }

    @Test
    void retriesABusyExclusiveSubscription() throws Exception {
        AtomicInteger attempts = new AtomicInteger();

        retry(0.5)
                .call(
                        "subscribe",
                        PARTITION,
                        () -> {
                            if (attempts.incrementAndGet() == 1) {
                                throw new ConsumerBusyException(
                                        "Exclusive consumer is already connected");
                            }
                            return null;
                        });

        assertThat(attempts).hasValue(2);
    }

    @Test
    void throwsTheLastFailureAfterSixRetries() {
        AtomicInteger attempts = new AtomicInteger();
        TimeoutException lastFailure = new TimeoutException("subscribe timed out");

        assertThatThrownBy(
                        () ->
                                retry(0.5)
                                        .call(
                                                "subscribe",
                                                PARTITION,
                                                () -> {
                                                    attempts.incrementAndGet();
                                                    throw lastFailure;
                                                }))
                .isSameAs(lastFailure);

        assertThat(attempts).hasValue(7);
        assertThat(sleeps).containsExactly(1_000L, 2_000L, 4_000L, 8_000L, 16_000L, 30_000L);
    }

    @Test
    void doesNotRetryFailuresThatPulsarTreatsAsPermanent() {
        AtomicInteger attempts = new AtomicInteger();

        assertThatThrownBy(
                        () ->
                                retry(0.5)
                                        .call(
                                                "subscribe",
                                                PARTITION,
                                                () -> {
                                                    attempts.incrementAndGet();
                                                    throw new TopicDoesNotExistException("gone");
                                                }))
                .isInstanceOf(TopicDoesNotExistException.class);

        assertThat(attempts).hasValue(1);
        assertThat(sleeps).isEmpty();
    }

    @Test
    void classifiesRetriableErrors() {
        assertThat(ConsumerSetupRetry.isRetriable(closedChannel())).isTrue();
        assertThat(ConsumerSetupRetry.isRetriable(new TimeoutException("timeout"))).isTrue();
        assertThat(ConsumerSetupRetry.isRetriable(new ConsumerBusyException("busy"))).isTrue();
        assertThat(ConsumerSetupRetry.isRetriable(new NotFoundException("missing"))).isFalse();
    }

    @Test
    void jitterStaysWithinHalfOfTheBackoffAndUnderTheCap() {
        assertThat(retry(0.0).backoffMs(0)).isEqualTo(500L);
        assertThat(retry(0.75).backoffMs(0)).isEqualTo(1_250L);
        assertThat(retry(0.0).backoffMs(3)).isEqualTo(4_000L);
        assertThat(retry(0.75).backoffMs(4)).isEqualTo(20_000L);
        assertThat(retry(0.0).backoffMs(5)).isEqualTo(15_000L);
        assertThat(retry(0.75).backoffMs(5)).isEqualTo(30_000L);
        assertThat(retry(0.75).backoffMs(40)).isEqualTo(30_000L);
    }

    @Test
    void stopsWhenInterruptedWhileWaiting() {
        ConsumerSetupRetry retry =
                new ConsumerSetupRetry(
                        6,
                        1_000,
                        30_000,
                        () -> 0.5,
                        millis -> {
                            throw new InterruptedException();
                        });

        assertThatThrownBy(
                        () ->
                                retry.call(
                                        "subscribe",
                                        PARTITION,
                                        () -> {
                                            throw closedChannel();
                                        }))
                .isInstanceOf(InterruptedException.class);
    }

    /** A retry whose jitter always returns the given value, recording every wait. */
    private ConsumerSetupRetry retry(double jitter) {
        return new ConsumerSetupRetry(6, 1_000, 30_000, () -> jitter, sleeps::add);
    }

    /** What a subscribe on a connection that another consumer closed looks like. */
    private static PulsarClientException closedChannel() {
        return PulsarClientException.unwrap(new ExecutionException(new ClosedChannelException()));
    }
}
