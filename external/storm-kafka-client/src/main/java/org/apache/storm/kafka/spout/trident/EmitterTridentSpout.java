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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutTuplesBuilder;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.IOpaquePartitionedTridentSpout;
import org.apache.storm.trident.topology.TransactionAttempt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.EARLIEST;
import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.LATEST;
import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.UNCOMMITTED_EARLIEST;
import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.UNCOMMITTED_LATEST;

public class EmitterTridentSpout<K,V> implements IOpaquePartitionedTridentSpout.Emitter<List<TopicPartition>, TopicPartitionTridentSpout, KafkaTridentSpoutBatchMetadata<K,V>>, Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(EmitterTridentSpout.class);

    private final KafkaSpoutConfig<K, V> kafkaSpoutConfig;
    private KafkaManagerTridentSpout<K, V> kafkaManager;
    private final KafkaConsumer<K, V> kafkaConsumer;
    private final KafkaSpoutTuplesBuilder<K, V> tuplesBuilder;
    private long pollTimeoutMs;
    private KafkaSpoutConfig.FirstPollOffsetStrategy firstPollOffsetStrategy;

    public EmitterTridentSpout(KafkaManagerTridentSpout<K,V> kafkaManager) {
        this.kafkaManager = kafkaManager;
        this.kafkaManager.subscribeKafkaConsumer();
        kafkaConsumer = kafkaManager.getKafkaConsumer();
        tuplesBuilder = kafkaManager.getTuplesBuilder();
        kafkaSpoutConfig = kafkaManager.getKafkaSpoutConfig();
        pollTimeoutMs = kafkaSpoutConfig.getPollTimeoutMs();
        firstPollOffsetStrategy = kafkaSpoutConfig.getFirstPollOffsetStrategy();
        LOG.debug("Created {}", this);
    }

    @Override
    public KafkaTridentSpoutBatchMetadata<K,V> emitPartitionBatch(TransactionAttempt tx, TridentCollector collector,
                                                                  TopicPartitionTridentSpout partitionTs, KafkaTridentSpoutBatchMetadata<K,V> lastBatch) {
        LOG.debug("Emitting batch for partition: [partition = {}], [transaction = {}], [collector = {}], [lastMetadata = {}]",
                partitionTs, tx, collector, lastBatch);

        final TopicPartition topicPartition = partitionTs.getTopicPartition();

        KafkaTridentSpoutBatchMetadata<K,V> currentBatch = lastBatch;

        TopicPartition[] pausedTopicPartitions = new TopicPartition[]{};
        try {
            pausedTopicPartitions = pauseTopicPartitions(topicPartition);   // only poll from current topic partition

            seek(topicPartition, lastBatch);

            // poll
            final ConsumerRecords<K, V> records = kafkaConsumer.poll(pollTimeoutMs);
            LOG.debug("Polled [{}] records from Kafka.", records.count());

            if (!records.isEmpty()) {
                emit(collector, records);
                // build new metadata
                currentBatch = new KafkaTridentSpoutBatchMetadata<>(topicPartition, records, lastBatch);
            }
        } finally {
            kafkaConsumer.resume(pausedTopicPartitions);
            LOG.trace("Resumed topic partitions [{}]", Arrays.toString(pausedTopicPartitions));
        }
        LOG.debug("Current metadata {}", currentBatch);
        return currentBatch;
    }

    private void emit(TridentCollector collector, ConsumerRecords<K, V> records) {
        for (ConsumerRecord<K, V> record : records) {
            final List<Object> tuple = tuplesBuilder.buildTuple(record);
            collector.emit(tuple);
            LOG.debug("Emitted tuple [{}] for record: [{}]", tuple, record);
        }
    }

    // TODO Refactor this code
    private long seek(TopicPartition tp, KafkaTridentSpoutBatchMetadata<K, V> lastBatchMeta) {
        long fetchOffset;
        if (lastBatchMeta != null) {
            kafkaConsumer.seek(tp, lastBatchMeta.getLastOffset() + 1);  // seek next offset after last offset from previous batch
            fetchOffset = kafkaConsumer.position(tp);
        } else {
            final OffsetAndMetadata committedOffset = kafkaConsumer.committed(tp);
            if (committedOffset != null) {             // offset was committed for this TopicPartition
                if (firstPollOffsetStrategy.equals(EARLIEST)) {
                    kafkaConsumer.seekToBeginning(tp);
                    fetchOffset = kafkaConsumer.position(tp);
                } else if (firstPollOffsetStrategy.equals(LATEST)) {
                    kafkaConsumer.seekToEnd(tp);
                    fetchOffset = kafkaConsumer.position(tp);
                } else {
                    // By default polling starts at the last committed offset. +1 to point fetch to the first uncommitted offset.
                    fetchOffset = committedOffset.offset() + 1;
                    kafkaConsumer.seek(tp, fetchOffset);
                }
            } else {    // no commits have ever been done, so start at the beginning or end depending on the strategy
                if (firstPollOffsetStrategy.equals(EARLIEST) || firstPollOffsetStrategy.equals(UNCOMMITTED_EARLIEST)) {
                    kafkaConsumer.seekToBeginning(tp);
                } else if (firstPollOffsetStrategy.equals(LATEST) || firstPollOffsetStrategy.equals(UNCOMMITTED_LATEST)) {
                    kafkaConsumer.seekToEnd(tp);
                }
                fetchOffset = kafkaConsumer.position(tp);
            }
        }
        return fetchOffset;
    }

    // returns paused topic partitions
    private TopicPartition[] pauseTopicPartitions(TopicPartition excludedTp) {
        final Set<TopicPartition> assignedTopicPartitions  = new HashSet<>(kafkaConsumer.assignment());
        LOG.debug("Currently assigned topic partitions [{}]", assignedTopicPartitions);
        assignedTopicPartitions.remove(excludedTp);

        final TopicPartition[] pausedTopicPartitions = new TopicPartition[assignedTopicPartitions.size()];
        kafkaConsumer.pause(assignedTopicPartitions.toArray(pausedTopicPartitions));
        LOG.trace("Paused topic partitions [{}]", Arrays.toString(pausedTopicPartitions));
        return pausedTopicPartitions;
    }

    @Override
    public void refreshPartitions(List<TopicPartitionTridentSpout> partitionResponsibilities) {

    }

    @Override
    public List<TopicPartitionTridentSpout> getOrderedPartitions(List<TopicPartition> allPartitionInfo) {
        final List<TopicPartitionTridentSpout> topicPartitionsTrident = new ArrayList<>(allPartitionInfo == null ? 0 : allPartitionInfo.size());
        if (allPartitionInfo != null) {
            for (TopicPartition topicPartition : allPartitionInfo) {
                topicPartitionsTrident.add(new TopicPartitionTridentSpout(topicPartition));
            }
        }
        LOG.debug("OrderedPartitions = {}", topicPartitionsTrident);
        return topicPartitionsTrident;
    }

    @Override
    public void close() {

    }

    @Override
    public String toString() {
        return "MyEmitter{" +
                "kafkaSpoutConfig=" + kafkaSpoutConfig +
                ", kafkaManager=" + kafkaManager +
                ", kafkaConsumer=" + kafkaConsumer +
                ", tuplesBuilder=" + tuplesBuilder +
                '}';
    }
}
