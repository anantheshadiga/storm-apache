/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.storm.kafka.spout.strategy;

import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class KafkaConfig<K, V> {
    public static final long DEFAULT_POLL_TIMEOUT = 500;

    private final Map<String, Object> configs;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;
    private final long pollTimeout;

    public KafkaConfig(Builder<K,V> builder) {
        this.configs = builder.configs;
        this.keyDeserializer = builder.keyDeserializer;
        this.valueDeserializer = builder.valueDeserializer;
        this.pollTimeout = builder.pollTimeout;
    }

    public static class Builder<K,V> {
        private Map<String, Object> configs;
        private Deserializer<K> keyDeserializer;
        private Deserializer<V> valueDeserializer;
        private long pollTimeout = DEFAULT_POLL_TIMEOUT;

        public void setConfigs(Map<String, Object> configs) {
            this.configs = configs;
        }

        public void setKeyDeserializer(Deserializer<K> keyDeserializer) {
            this.keyDeserializer = keyDeserializer;
        }

        public void setValueDeserializer(Deserializer<V> valueDeserializer) {
            this.valueDeserializer = valueDeserializer;
        }

        public void setPollTimeout(long pollTimeout) {
            this.pollTimeout = pollTimeout;
        }

        public KafkaConfig<K,V> build() {
            return new KafkaConfig<>(this);
        }
    }

    public Map<String, Object> getConfigs() {
        return configs;
    }

    public Deserializer<K> getKeyDeserializer() {
        return keyDeserializer;
    }

    public Deserializer<V> getValueDeserializer() {
        return valueDeserializer;
    }

    public long getPollTimeout() {
        return pollTimeout;
    }
}
