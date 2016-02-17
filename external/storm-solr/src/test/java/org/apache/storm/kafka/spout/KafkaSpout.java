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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.Config;
import org.apache.storm.kafka.spout.strategy.KafkaConfig;
import org.apache.storm.kafka.spout.strategy.StreamBuilder;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class KafkaSpout<K,V> extends BaseRichSpout {
    private static final Logger log = LoggerFactory.getLogger(KafkaSpout.class);

    private Map conf;
    private TopologyContext context;
    protected SpoutOutputCollector collector;
    private final KafkaConfig<K, V> kafkaConfig;
    private KafkaConsumer<K, V> kafkaConsumer;
    private long waitTime;
    private long maxWaitTime;
    private final ConcurrentMap<TopicPartition, OffsetAndMetadata> acked = new ConcurrentHashMap<>();   //TODO: ConcurrentHashMap
    private Map<String, String> failed;
    private ConcurrentMap<TopicPartition, Iterable<ConsumerRecord<K, V>>> polled = new ConcurrentHashMap<>();
    private int numPendingRecords = 0;

    private StreamBuilder<K,V> streamBuilder;

    public KafkaSpout(KafkaConfig<K,V> kafkaConfig) {
        this.kafkaConfig = kafkaConfig;                 // Pass in configuration
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.conf = conf;
        this.context = context;
        this.collector = collector;
        this.kafkaConsumer = new KafkaConsumer<>(kafkaConfig.getConfigs(), kafkaConfig.getKeyDeserializer(), kafkaConfig.getValueDeserializer());

        List<String> topics = new ArrayList<>();    // TODO
        subscribe(kafkaConsumer);
        kafkaConsumer.subscribe(topics);
    }



    //TODO HANDLE PARALLELISM
//    String topologyMaxSpoutPending = Config.TOPOLOGY_MAX_SPOUT_PENDING;
    @Override
    public void nextTuple() {
        commitAckedRecords();

        if (!failedTuples.isEmpty()) {
            retryFailedTuples();
        } else {
            pollNewRecordsAndEmitTuples();
        }

        // poll new records
        if (getFailedRecords() + getNumPendingRecords() < getMaxPendingRecords()) {
            ConsumerRecords<K, V> consumerRecords = kafkaConsumer.poll(kafkaConfig.getPollTimeout());
            numPendingRecords += consumerRecords.count();

            for (TopicPartition tp : consumerRecords.partitions()) {
                polled.put(tp, consumerRecords.records(tp.topic()));
            }
        }

        // Emmit records if we have over the maxPendingRecords, or maxWaitTime has exceeded
        if (getFailedRecords() + getNumPendingRecords() < getMaxPendingRecords() && getWaitTime() > getMaxWaitTime()) {
            emitPolledTuples(); // Emmit polled tuples which includes retrying failed


            collector.emit(getStreamId(), buildTuple(), getMessageId());
        }

        // emit when numPendingRecords > maxPendingTuples || wait time > maxWaitTime
        // new tuples
        // failed tuples

        List<Values> messagesList = new ArrayList<Values>();

        collector.emit(messagesList);
    }

    @Override
    public void ack(Object msgId) {
        final MessageId messageId = (MessageId) msgId;
        final String metadata = messageId.buildMetadata(Thread.currentThread());
        acked.put(new TopicPartition(messageId.topic, messageId.partition),
                new OffsetAndMetadata(messageId.offset, metadata));
        log.debug("Acked {}", metadata);
        offsetManager.ack(messageId);
        offsetManager.commitReadyOffsets();
    }

    Map<TopicPartition, OffsetManager> offsetManagers;

    private OffsetManager offsetManager;

    private class OffsetManager {
        Map<TopicPartition, OffsetManager> partitionToOffset;

        public void ack(MessageId messageId) {
            final TopicPartition tp = new TopicPartition(messageId.topic, messageId.partition);
            if (!partitionToOffset.containsKey(tp)) {
                partitionToOffset.put(tp, new OffsetManager(messageId));
            }
            OffsetManager om = partitionToOffset.get(tp);
            om.ack(messageId);
        }

        /** Commits to kafka the maximum sequence of continuous offsets that have been acked for a partition */
        public void commitReadyOffsets() {
            final Map<TopicPartition, OffsetAndMetadata> topicPartitionToOffset = new HashMap<>();
            for (TopicPartition tp : partitionToOffset.keySet()) {
                partitionToOffset.get(tp).commitReadyOffsets();
            }

            kafkaConsumer.commitSync(topicPartitionToOffset);

        }

    }

    private class OffsetManagerEntry {
        private long lastCommittedOffset = 0;
        private List<Long> offsetsSublist = new ArrayList<>();      // in root keep only two offsets - first and last
        private OffsetManagerEntry prev;
        private OffsetManagerEntry next;


        public void ack(MessageId messageId, OffsetManagerEntry prev, OffsetManagerEntry next) {
            //do merge

        }

        public void commitReadyOffsets() {
            if (isRoot() && !offsetsSublist.isEmpty() && offsetsSublist.get(0) == lastCommittedOffset + 1) {
                kafkaConsumer.commitSync();
            }

        }

        private boolean isRoot() {
            return prev == null;
        }
    }

    private void emitPolledTuples() {



        collector.emit(getStreamId(), buildTuple(failed), getMessageId());

    }

    //TODO: Null message id for no acking, which is good to use with enable.auto.commit=false
    private void pollNewRecordsAndEmitTuples() {
        ConsumerRecords<K, V> consumerRecords = kafkaConsumer.poll(kafkaConfig.getPollTimeout());
        for (TopicPartition tp : consumerRecords.partitions()) {
            final Iterable<ConsumerRecord<K, V>> records = consumerRecords.records(tp.topic());
            for (ConsumerRecord<K, V> record : records) {
                collector.emit(getStreamId(tp), buildTuple(tp, record), getMessageId(record));    // emits one tuple per record
            }
            polled.put(tp, records);
        }
    }

    private void emitFailedTuples() {

    }

    private void commit() {
        synchronized(acked) {
            kafkaConsumer.commitSync(acked);
            //remove from polled map
            //remove from acked
        }
    }

    private void getOutputFields1() {

    }

    private Values buildTuple(TopicPartition topicPartition, ConsumerRecord<K,V> consumerRecord) {
        return streamBuilder.buildTuple(topicPartition, consumerRecord);
    }

    private Object getMessageId(ConsumerRecord<K,V> consumerRecord) {
        return new MessageId(consumerRecord);
    }

    private void serialize() {

    }

    private String getStreamId(TopicPartition topicPartition) {
        return streamBuilder.getStream(topicPartition);
    }

    public int getFailedRecords() {
        return -1;
    }

    public int getNumPendingRecords() {
        return numPendingRecords;
    }

    //TODO confirm
    public int getMaxPendingRecords() {
        final String maxSpoutPending = (String) conf.get(Config.TOPOLOGY_MAX_SPOUT_PENDING);

        return maxSpoutPending != null ? Math.min(Integer.parseInt(maxSpoutPending), getMaxWaitTime());
    }

    public long getWaitTime() {
        return waitTime;
    }


    public long getMaxWaitTime() {
        return maxWaitTime;
    }
    private static class MessageId {
        private String topic;
        private int partition;
        private long offset;

        public MessageId(ConsumerRecord consumerRecord) {
            this(consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset());
        }

        public MessageId(String topic, int partition, long offset) {
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final MessageId messageId = (MessageId) o;
            if (partition != messageId.partition) {
                return false;
            }

            if (offset != messageId.offset) {
                return false;
            }

            return topic.equals(messageId.topic);
        }

        @Override
        public int hashCode() {
            int result = topic.hashCode();
            result = 31 * result + partition;
            result = 31 * result + (int) (offset ^ (offset >>> 32));
            return result;
        }
        public String buildMetadata(Thread currThread) {
            return "{" +
                    "topic='" + topic + '\'' +
                    ", partition=" + partition +
                    ", offset=" + offset +
                    ", thread='" + currThread.getName() + "'" +
                    '}';
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        if (kafkaSpoutStrategy.getDeclaredStreamsAndOutputFields() != null)     //TODO
            declarer.declare(getOutputFields());
        else {
            declarer.declareStream();
        }
    }

    public Fields getOutputFields() {
        return new Fields("kafka_field");
    }

    @Override
    public void fail(Object msgId) {
        final MessageId messageId = (MessageId) msgId;
        final String metadata = messageId.buildMetadata(Thread.currentThread());

        log.debug("Failed " + metadata);
    }

    @Override
    public void activate() {
        //resume processing
    }

    @Override
    public void deactivate() {
        //commit
    }

    @Override
    public void close() {
        //remove resources
    }
}
