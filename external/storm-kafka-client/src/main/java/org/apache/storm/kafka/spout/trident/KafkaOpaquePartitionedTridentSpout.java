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

package org.apache.storm.kafka.spout.trident;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutStreams;
import org.apache.storm.kafka.spout.KafkaSpoutTuplesBuilder;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.IOpaquePartitionedTridentSpout;
import org.apache.storm.trident.spout.ISpoutPartition;
import org.apache.storm.trident.topology.TransactionAttempt;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class KafkaOpaquePartitionedTridentSpout<K,V> implements IOpaquePartitionedTridentSpout {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaOpaquePartitionedTridentSpout.class);


    // Kafka
    private final KafkaSpoutConfig<K, V> kafkaSpoutConfig;
    private transient KafkaConsumer<K, V> kafkaConsumer;

    // Bookkeeping
    private KafkaSpoutStreams kafkaSpoutStreams;                        // Object that wraps all the logic to declare output fields and emit tuples
    private Set<TopicPartition> topicPartitions;
    private transient KafkaSpoutTuplesBuilder<K, V> tuplesBuilder;      // Object that contains the logic to build tuples for each ConsumerRecord


    public KafkaOpaquePartitionedTridentSpout(KafkaSpoutConfig<K, V> kafkaSpoutConfig) {
        this.kafkaSpoutConfig = kafkaSpoutConfig;                 // Pass in configuration
        this.kafkaSpoutStreams = kafkaSpoutConfig.getKafkaSpoutStreams();

        topicPartitions = new HashSet<>();

        // Tuples builder delegate
        tuplesBuilder = kafkaSpoutConfig.getTuplesBuilder();

        subscribeKafkaConsumer();
    }

    @Override
    public Emitter getEmitter(Map conf, TopologyContext context) {
        return new MyEmitter();
    }

    @Override
    public Coordinator getCoordinator(Map conf, TopologyContext context) {
        return new MyCoordinator();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return null;
    }

    class MyMeta {
        private String topic;
        private int partition = Integer.MIN_VALUE;

        long firstOffset = Long.MIN_VALUE;
        long lastOffset = Long.MIN_VALUE;

        public MyMeta(ConsumerRecords<K,V> consumerRecords) {
            for (ConsumerRecord<K, V> consumerRecord : consumerRecords) {
                if (topic == null ) {
                    topic = consumerRecord.topic();
                }
                if(partition == Integer.MIN_VALUE) {
                    partition = consumerRecord.partition();
                }

                long offset = consumerRecord.offset();

                if (firstOffset == Long.MIN_VALUE || firstOffset > offset) {
                    firstOffset = offset;
                }

                if (lastOffset == Long.MIN_VALUE || lastOffset < offset) {
                    lastOffset = offset;
                }
            }
        }

        @Override
        public String toString() {
            return "MyMeta{" +
                    "firstOffset=" + firstOffset +
                    ", lastOffset=" + lastOffset +
                    '}';
        }
    }

    private class MyEmitter implements Emitter<List<TopicPartition>, MyTopicPartition, MyMeta> {
        @Override
        public MyMeta emitPartitionBatch(TransactionAttempt tx, TridentCollector collector, MyTopicPartition partition, MyMeta lastPartitionMeta) {
            MyMeta currentPartitionMeta = lastPartitionMeta;
            final Set<TopicPartition> assignedTopicPartitions  = kafkaConsumer.assignment();

            LOG.debug("Currently assigned topic partitions [{}]", assignedTopicPartitions);

            assignedTopicPartitions.remove(partition.getTopicPartition());

            final TopicPartition[] pausedTopicPartitions = new TopicPartition[assignedTopicPartitions.size()];

            try {
                kafkaConsumer.pause(assignedTopicPartitions.toArray(pausedTopicPartitions));

                LOG.trace("Paused topic partitions [{}]", Arrays.toString(pausedTopicPartitions));

                final ConsumerRecords<K, V> records = kafkaConsumer.poll(kafkaSpoutConfig.getPollTimeoutMs());
                LOG.debug("Polled [{}] records from Kafka.", records.count());

                currentPartitionMeta = new MyMeta(records);

                for (ConsumerRecord<K, V> record : records) {
                    final List<Object> tuple = tuplesBuilder.buildTuple(record);
                    collector.emit(tuple);
                }
            } finally {
                kafkaConsumer.resume(pausedTopicPartitions);
                LOG.trace("Resumed topic partitions [{}]", Arrays.toString(pausedTopicPartitions));
            }
            return currentPartitionMeta;
        }
        @Override
        public void refreshPartitions(List<MyTopicPartition> partitionResponsibilities) {

        }

        @Override
        public List<MyTopicPartition> getOrderedPartitions(List<TopicPartition> allPartitionInfo) {
            List<MyTopicPartition> ltp = new ArrayList<>(allPartitionInfo == null ? 0 : allPartitionInfo.size());
            if (allPartitionInfo != null) {
                for (TopicPartition topicPartition : allPartitionInfo) {
                    ltp.add(new MyTopicPartition(topicPartition));
                }
            }
            return ltp;
        }

        @Override
        public void close() {

        }
    }

    private class MyCoordinator implements Coordinator<List<TopicPartition>> {
        @Override
        public boolean isReady(long txid) {
            return true;    // the "old" trident kafka spout is like this
        }

        @Override
        public List<TopicPartition> getPartitionsForBatch() {
            return new ArrayList<>(topicPartitions);
        }

        @Override
        public void close() {
            // the "old" trident kafka spout is like this
        }
    }

    private class MyTopicPartition implements ISpoutPartition {
        private TopicPartition topicPartition;

        public MyTopicPartition(String topic, int partition) {
            this(new TopicPartition(topic, partition));
        }

        public MyTopicPartition(TopicPartition topicPartition) {
            this.topicPartition = topicPartition;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MyTopicPartition that = (MyTopicPartition) o;

            return topicPartition != null ? topicPartition.equals(that.topicPartition) : that.topicPartition == null;

        }

        public TopicPartition getTopicPartition() {
            return topicPartition;
        }

        @Override
        public int hashCode() {
            return topicPartition != null ? topicPartition.hashCode() : 0;
        }

        @Override
        public String toString()  {
            return topicPartition.toString();
        }

        @Override
        public String getId() {
            return topicPartition.topic() + "/" + topicPartition.partition();
        }
    }

    private void subscribeKafkaConsumer() {
        kafkaConsumer = new KafkaConsumer<>(kafkaSpoutConfig.getKafkaProps(),
                kafkaSpoutConfig.getKeyDeserializer(), kafkaSpoutConfig.getValueDeserializer());
        kafkaConsumer.subscribe(kafkaSpoutConfig.getSubscribedTopics(), new KafkaSpoutConsumerRebalanceListener());
        // Initial poll to get the consumer registration process going.
        // KafkaSpoutConsumerRebalanceListener will be called following this poll, upon partition registration
        kafkaConsumer.poll(0);
    }

    private class KafkaSpoutConsumerRebalanceListener implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            LOG.debug("Partitions revoked. [consumer-group={}, consumer={}, topic-partitions={}]",
                    kafkaSpoutConfig.getConsumerGroupId(), kafkaConsumer, partitions);
            topicPartitions.removeAll(partitions);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            topicPartitions.addAll(partitions);
            LOG.debug("Partitions reassignment. [consumer-group={}, consumer={}, topic-partitions={}]",
                    kafkaSpoutConfig.getConsumerGroupId(), kafkaConsumer, partitions);
        }
    }
}
