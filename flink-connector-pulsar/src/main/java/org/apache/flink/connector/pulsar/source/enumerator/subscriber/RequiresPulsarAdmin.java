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

import org.apache.flink.annotation.Internal;

import org.apache.pulsar.client.admin.PulsarAdmin;

/**
 * Capability interface for {@link PulsarSubscriber} implementations that require PulsarAdmin for
 * their operations.
 *
 * <p>Subscribers that need to perform administrative operations (such as listing namespaces,
 * querying tenant metadata, etc.) should implement this interface. The PulsarSourceEnumerator will
 * detect this capability and inject a PulsarAdmin instance during initialization.
 *
 * <p>The PulsarAdmin instance is owned and managed by the enumerator - subscribers should not close
 * it.
 *
 * <p>Example usage: {@link
 * org.apache.flink.connector.pulsar.source.enumerator.subscriber.impl.NamespacePatternSubscriber}
 * implements this interface to perform dynamic namespace discovery.
 */
@Internal
public interface RequiresPulsarAdmin {

    /**
     * Inject the PulsarAdmin instance for administrative operations.
     *
     * <p>This method is called by the PulsarSourceEnumerator during initialization, after {@link
     * PulsarSubscriber#open(org.apache.pulsar.client.api.PulsarClient)} has been called.
     *
     * @param admin the PulsarAdmin instance to use for administrative operations. Must not be null.
     */
    void setAdmin(PulsarAdmin admin);
}
