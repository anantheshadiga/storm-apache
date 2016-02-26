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

package org.apache.storm.kafka.spout;

import org.apache.kafka.common.serialization.Deserializer;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * KafkaSpoutConfig defines the required configuration to connect a consumer to a consumer group, as well as the subscribing topics
 */
public class KafkaSpoutConfig<K, V> implements Serializable {
    public static final long DEFAULT_POLL_TIMEOUT_MS = 2_000;   // 2s
    public static final long DEFAULT_COMMIT_FREQ_MS = 15_000;   // 15s
    public static final int DEFAULT_MAX_RETRIES = Integer.MAX_VALUE;

    // Kafka property names
    public interface Consumer {
        String ENABLE_AUTO_COMMIT = "enable.auto.commit";
        String GROUP_ID = "group.id";
        String BOOTSTRAP_SERVERS = "bootstrap.servers";
        String AUTO_COMMIT_INTERVAL_MS = "auto.commit.interval.ms";
        String SESSION_TIMEOUT_MS = "session.timeout.ms";
        String KEY_DESERIALIZER = "key.deserializer";
        String VALUE_DESERIALIZER = "value.deserializer";
    }

    /**
     * The offset used by the Kafka spout in the first poll to Kafka broker. The choice of this parameter will
     * affect the number of consumer records returned in the first poll. By default this parameter set to UNCOMMITTED_EARLIEST. <br/><br/>
     * The allowed values are EARLIEST, LATEST, UNCOMMITTED_EARLIEST, UNCOMMITTED_LATEST. <br/>
     * <ul>
     * <li>EARLIEST means that the kafka spout polls records starting in the first offset of the partition.</li>
     * <li>LATEST means that the kafka spout polls records with offsets greater than the last offset in the partition.</li>
     * <li>UNCOMMITTED_EARLIEST means that the kafka spout polls records from the last committed offset, if any.
     * If no offset has been committed, it behaves as EARLIEST.</li>
     * <li>UNCOMMITTED_LATEST means that the kafka spout polls records from the last committed offset, if any.<br/>
     * If no offset has been committed, it behaves as LATEST.</li>
     * </ul>
     * */
    public enum FirstPollOffset {
        EARLIEST,
        LATEST,
        UNCOMMITTED_EARLIEST,
        UNCOMMITTED_LATEST }

    // Kafka consumer configuration
    private final Map<String, Object> kafkaProps;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;
    private List<String> topics;
    private FirstPollOffset firstPollOffset;
    private final long pollTimeoutMs;

    // Kafka spout configuration
    private long commitFreqMs;
    private int maxRetries;

    private KafkaSpoutConfig(Builder<K,V> builder) {
        this.kafkaProps = builder.kafkaProps;
        this.keyDeserializer = builder.keyDeserializer;
        this.valueDeserializer = builder.valueDeserializer;
        this.pollTimeoutMs = builder.pollTimeoutMs;
        this.commitFreqMs = builder.commitFreqMs;
        this.topics = builder.topics;
        this.maxRetries = builder.maxRetries;
        this.firstPollOffset = builder.firstPollOffset;
    }

    public static class Builder<K,V> {
        private Map<String, Object> kafkaProps;
        private Deserializer<K> keyDeserializer;
        private Deserializer<V> valueDeserializer;
        private long pollTimeoutMs = DEFAULT_POLL_TIMEOUT_MS;
        private long commitFreqMs = DEFAULT_COMMIT_FREQ_MS;
        private int maxRetries = DEFAULT_MAX_RETRIES;
        private List<String> topics;
        private FirstPollOffset firstPollOffset = FirstPollOffset.UNCOMMITTED_EARLIEST;

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
        public Builder<K,V> setKeyDeserializer(Deserializer<K> keyDeserializer) {
            this.keyDeserializer = keyDeserializer;
            return this;
        }

        /**
         * Specifying this value deserializer overrides the property value.deserializer
         */
        public Builder<K,V> setValueDeserializer(Deserializer<V> valueDeserializer) {
            this.valueDeserializer = valueDeserializer;
            return this;
        }

        /**
         * Specifies the time, in milliseconds, spent waiting in poll if data is not available. Default is 15s
         * @param pollTimeoutMs time in ms
         */
        public Builder<K,V> setPollTimeoutMs(long pollTimeoutMs) {
            this.pollTimeoutMs = pollTimeoutMs;
            return this;
        }

        /**
         * Specifies the frequency, in milliseconds, the offset commit task is called
         * @param commitFreqMs time in ms
         */
        public Builder<K,V> setCommitFreqMs(long commitFreqMs) {
            this.commitFreqMs = commitFreqMs;
            return this;
        }

        /**
         * Defines the max number of retrials in case of tuple failure. The default is to retry forever, which means that
         * no new records are polled until the previous polled records have been acked. This guarantees at once delivery of
         * all the previously polled records.
         * By specifying a finite value for maxRetries, the user decides to sacrifice guarantee of delivery for the previous
         * polled records in favor of processing more records.
         * @param maxRetries max number of retrials
         */
        public Builder<K,V> setMaxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        /**
         * Sets the offset used by the Kafka spout in the first poll to Kafka broker upon process start.
         * Please refer to to the documentation in {@link FirstPollOffset}
         * @param firstPollOffset Offset used by Kafka spout first poll
         * */
        public Builder<K, V> setFirstPollOffset(FirstPollOffset firstPollOffset) {
            this.firstPollOffset = firstPollOffset;
            return this;
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

    public long getOffsetsCommitFreqMs() {
        return commitFreqMs;
    }

    public boolean isConsumerAutoCommitMode() {
        return kafkaProps.get(Consumer.ENABLE_AUTO_COMMIT) == null     // default is true
                || ((String)kafkaProps.get(Consumer.ENABLE_AUTO_COMMIT)).equalsIgnoreCase("true");
    }

    public String getConsumerGroupId() {
        return (String) kafkaProps.get(Consumer.GROUP_ID);
    }

    public List<String> getSubscribedTopics() {
        return Collections.unmodifiableList(topics);
    }

    public int getMaxTupleRetries() {
        return maxRetries;
    }

    public FirstPollOffset getFirstPollOffset() {
        return firstPollOffset;
    }
}
