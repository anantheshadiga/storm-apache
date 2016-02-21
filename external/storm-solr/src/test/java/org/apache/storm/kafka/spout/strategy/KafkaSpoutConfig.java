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

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * KafkaSpoutConfig defines the required configuration to connect a consumer to a consumer group, as well as the subscribing topics
 */
public class KafkaSpoutConfig<K, V> {
    public static final long DEFAULT_POLL_TIMEOUT_MS = 2_000;   // 2s
    public static final long DEFAULT_COMMIT_FREQ_MS = 15_000;   // 15s
    public static final int DEFAULT_MAX_RETRIES = Integer.MAX_VALUE;   // 15s

    public static String CONSUMER_AUTO_COMMIT_ENABLE = "auto.commit.enable";
    public static String CONSUMER_GROUP_ID = "group.id";

    private final Map<String, Object> kafkaProps;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;
    private List<String> topics;

    private final long pollTimeoutMs;
    private long commitFreqMs;
    private int maxRetries;


    public KafkaSpoutConfig(Builder<K,V> builder) {
        this.kafkaProps = builder.kafkaProps;
        this.keyDeserializer = builder.keyDeserializer;
        this.valueDeserializer = builder.valueDeserializer;
        this.pollTimeoutMs = builder.pollTimeoutMs;
        this.commitFreqMs = builder.commitFreqMs;
        this.topics = builder.topics;
        this.maxRetries = builder.maxRetries;
    }

    public static class Builder<K,V> {
        private Map<String, Object> kafkaProps;
        private Deserializer<K> keyDeserializer;
        private Deserializer<V> valueDeserializer;
        private long pollTimeoutMs = DEFAULT_POLL_TIMEOUT_MS;
        private long commitFreqMs = DEFAULT_COMMIT_FREQ_MS;
        private int maxRetries = DEFAULT_MAX_RETRIES;
        private List<String> topics;

        /***
         * KafkaSpoutConfig defines the required configuration to connect a consumer to a consumer group, as well as the subscribing topics
         * The optional configuration can be specified using the set methods of this builder
         * @param kafkaProps    properties defining consumer connection to Kafka broker as specified in @see <a href="http://kafka.apache.org/090/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html">KafkaConsumer</a>
         * @param topics    List of topics subscribing the consumer group
         */
        public Builder(Map<String, Object> kafkaProps, List<String> topics) {
            if (kafkaProps == null || kafkaProps.isEmpty()) {
                throw new IllegalArgumentException("Properties defining consumer connection to Kafka broker are required. " + kafkaProps);
            }

            if (topics == null || topics.isEmpty())  {
                throw new IllegalArgumentException("List of topics subscribing the consumer group is required. " + topics);
            }

            this.kafkaProps = kafkaProps;
            this.topics = topics;
        }

        /**
         * Specifying this key deserializer overrides the property key.deserializer
         */
        public void setKeyDeserializer(Deserializer<K> keyDeserializer) {
            this.keyDeserializer = keyDeserializer;
        }

        /**
         * Specifying this value deserializer overrides the property value.deserializer
         */
        public void setValueDeserializer(Deserializer<V> valueDeserializer) {
            this.valueDeserializer = valueDeserializer;
        }

        /**
         * Specifies the time, in milliseconds, spent waiting in poll if data is not available. Default is 15s
         * @param pollTimeoutMs time in ms
         */
        public void setPollTimeoutMs(long pollTimeoutMs) {
            this.pollTimeoutMs = pollTimeoutMs;
        }

        /**
         * Specifies the frequency, in milliseconds, the offset commit task is called
         * @param commitFreqMs time in ms
         */
        public void setCommitFreqMs(long commitFreqMs) {
            this.commitFreqMs = commitFreqMs;
        }

        /**
         * Defines the max number of retrials in case of tuple failure. The default is to retry forever, which means that
         * no new records are polled until the previous polled records have been acked. This guarantees at once delivery of
         * all the previously polled records.
         * By specifying a finite value for maxRetries, the user decides to sacrifice guarantee of delivery for the previous
         * polled records in favor of processing more records.
         * @param maxRetries max number of retrials
         */
        public void setMaxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
        }

        public KafkaSpoutConfig<K,V> build() {
            return new KafkaSpoutConfig<>(this);
        }
    }

    public Map<String, Object> getKafkaProps() {
        return kafkaProps;
    }

    public Deserializer<K> getKeyDeserializer() {
        return keyDeserializer;
    }

    public Deserializer<V> getValueDeserializer() {
        return valueDeserializer;
    }

    public long getPollTimeoutMs() {
        return pollTimeoutMs;
    }

    public long getOffsetCommitFreqMs() {
        return commitFreqMs;
    }

    public boolean isConsumerAutoCommitMode() {
        return kafkaProps == null || kafkaProps.get(CONSUMER_AUTO_COMMIT_ENABLE) == null     // default is true
                || ((String)kafkaProps.get(CONSUMER_AUTO_COMMIT_ENABLE)).equalsIgnoreCase("true");
    }

    public String getConsumerGroupId() {
        return (String) kafkaProps.get(CONSUMER_GROUP_ID);
    }

    public List<String> getTopics() {
        return Collections.unmodifiableList(topics);
    }

    public int getMaxRetries() {
        return maxRetries;
    }
}
