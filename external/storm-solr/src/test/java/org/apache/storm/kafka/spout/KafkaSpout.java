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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    private OffsetsManager offsetsManager;

    private StreamBuilder<K,V> streamBuilder;

    public KafkaSpout(KafkaConfig<K,V> kafkaConfig) {
        this.kafkaConfig = kafkaConfig;                 // Pass in configuration
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.conf = conf;
        this.context = context;
        this.collector = collector;
        kafkaConsumer = new KafkaConsumer<>(kafkaConfig.getConfigs(), kafkaConfig.getKeyDeserializer(), kafkaConfig.getValueDeserializer());
        offsetsManager = new OffsetsManager();


        List<String> topics = new ArrayList<>();    // TODO
        subscribe(kafkaConsumer);
        kafkaConsumer.subscribe(topics);
    }

    //TODO HANDLE PARALLELISM
//    String topologyMaxSpoutPending = Config.TOPOLOGY_MAX_SPOUT_PENDING;
    @Override
    public void nextTuple() {
        commitAckedRecords();

        if (retry()) {
            retryFailedTuples();
        } else {
            pollNewRecordsAndEmitTuples();
        }
    }

    private boolean retry() {
        return offsetsManager.retry();
    }

    private void retryFailedTuples() {
        offsetsManager.retryFailed();
    }

    private void commitAckedRecords() {
        offsetsManager.commitAckedOffsets();
    }

    //TODO: Null message id for no acking, which is good to use with enable.auto.commit=false
    private void pollNewRecordsAndEmitTuples() {
        ConsumerRecords<K, V> consumerRecords = kafkaConsumer.poll(kafkaConfig.getPollTimeout());
        log.debug("Polled {[]} records from Kafka", consumerRecords.count());
        for (TopicPartition tp : consumerRecords.partitions()) {
            final Iterable<ConsumerRecord<K, V>> records = consumerRecords.records(tp.topic());     // TODO Decide if emmit per topic or per partition
            for (ConsumerRecord<K, V> record : records) {
                collector.emit(getStreamId(tp), buildTuple(tp, record), createMessageId(record));    // emits one tuple per record
            }
        }
    }

    @Override
    public void ack(Object msgId) {
        offsetsManager.ack((MessageId) msgId);
    }

    //TODO: HANDLE CONSUMER REBALANCE

    @Override
    public void fail(Object msgId) {
        offsetsManager.fail((MessageId) msgId);
    }

    private class OffsetsManager {
        final Map<TopicPartition, OffsetEntry> acked = new HashMap<>();
        final Map<TopicPartition, Set<MessageId>> failed = new HashMap<>();

        public void ack(MessageId msgId) {
            final TopicPartition tp = new TopicPartition(msgId.topic, msgId.partition);

            if (!acked.containsKey(tp)) {
                acked.put(tp, new OffsetEntry(null, null));
            }
            acked.get(tp).ack(msgId);

            // if acked message is a retry, remove it from failed data structure
            if (failed.containsKey(tp)) {
                final Set<MessageId> messageIds = failed.get(tp);
                messageIds.remove(msgId);
                if (messageIds.isEmpty()) {
                    failed.remove(tp);
                }
            }
        }

        public void fail(MessageId msgId) {
            final TopicPartition tp = new TopicPartition(msgId.topic, msgId.partition);
            if (!failed.containsKey(tp)) {
                failed.put(tp, new HashSet<MessageId>());
            }

            final Set<MessageId> msgIds = failed.get(tp);
            msgId.incNumFails();        // increment number of failures counter
            msgIds.add(msgId);

            // limit to max number of retries
            if (msgId.numFails >= maxRetries()) {
                log.debug("Reached the maximum number of retries. Adding message {[]} to list of messages to be committed to kafka", msgId);
                msgIds.remove(msgId);
                if (msgIds.isEmpty()) {
                    failed.remove(tp);
                }
            }
        }

        public boolean retry() {
            return failed.size() != 0;
        }

        public void retryFailed() {
            for (TopicPartition tp: failed.keySet()) {
                for (MessageId msgId : failed.get(tp)) {
                    collector.emit(getStreamId(tp), buildTuple(tp, ), msgId);
                }
            }
        }

        // TODO
        private int maxRetries() {
            return Integer.MAX_VALUE;
        }

        /** Commits to kafka the maximum sequence of continuous offsets that have been acked for a partition */
        public void commitAckedOffsets() {
            final Map<TopicPartition, OffsetAndMetadata> ackedOffsets = new HashMap<>();
            for (TopicPartition tp : acked.keySet()) {
                final MessageId msgId = acked.get(tp).getMaxOffsetMsgAcked();
                ackedOffsets.put(tp, new OffsetAndMetadata(msgId.offset, msgId.metadata(Thread.currentThread())));
            }

            kafkaConsumer.commitSync(ackedOffsets);

            cleanMap();
            // have to clean topic partitions if that is the case
        }

        private void cleanMap() {

        }
    }

    private class OffsetEntry {
        private long lastCommittedOffset = 0;
        private List<MessageId> offsets = new ArrayList<>();      // in root keep only two offsets - first and last
        private OffsetEntry prev;
        private OffsetEntry next;

        public OffsetEntry(OffsetEntry prev, OffsetEntry next) {
            this.prev = prev;
            this.next = next;
        }

        public void ack(MessageId msgId) {
            ack(msgId, this);
            //do merge
        }

        //TODO: Make it Iterative to be faster
        private void ack(MessageId msgId, OffsetEntry offsetEntry) {
            if (offsetEntry == null) {
                return;
            }

            if (msgId.offset == (getLastOffset() + 1)) {    // msgId becomes last element of this sublist
                setLast(msgId);
            } else if (msgId.offset < getFirstOffset()) {   // msgId
                setFirst(msgId);
            } else {
                ack(msgId, offsetEntry.next);
            }
            //TODO merge
        }


        public MessageId getMaxOffsetMsgAcked() {
            MessageId msgId = null;
            if (isRoot() && !offsets.isEmpty() && offsets.get(0).offset == lastCommittedOffset + 1) {
                msgId = offsets.get(offsets.size() - 1);
                lastCommittedOffset = msgId.offset;
            }
            return msgId;
        }

        private long getLastOffset() {
            return offsets.isEmpty() ? -1 : offsets.get(offsets.size() - 1).offset;
        }

        private long getFirstOffset() {
            return offsets.isEmpty() ? -1 : offsets.get(0).offset;
        }

        private void setLast(MessageId msgId) {
            offsets.set(offsets.isEmpty() ? 0 : offsets.size() - 1, msgId);
        }

        private void setFirst(MessageId msgId) {
            offsets.set(0, msgId);
        }

        private boolean isRoot() {
            return prev == null;
        }
    }

    private Values buildTuple(TopicPartition topicPartition, ConsumerRecord<K,V> consumerRecord) {
        return streamBuilder.buildTuple(topicPartition, consumerRecord);
    }

    private Object createMessageId(ConsumerRecord<K,V> consumerRecord) {
        return new MessageId(consumerRecord);
    }

    private void serialize() {

    }

    private String getStreamId(TopicPartition topicPartition) {
        return streamBuilder.getStream(topicPartition);
    }

    private static class MessageId {
        public static final OffsetComparator OFFSET_COMPARATOR = new OffsetComparator();
        String topic;
        int partition;
        private long offset;
        int numFails = 0;

        public MessageId(ConsumerRecord consumerRecord) {
            this(consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset());
        }

        public MessageId(String topic, int partition, long offset) {
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
        }

        public void incNumFails() {
            ++numFails;
        }

        public String metadata(Thread currThread) {
            return "{" +
                    "topic='" + topic + '\'' +
                    ", partition=" + partition +
                    ", offset=" + offset +
                    ", numFails=" + numFails +
                    ", thread='" + currThread.getName() + "'" +
                    '}';
        }

        @Override
        public String toString() {
            return "MessageId{" +
                    "topic='" + topic + '\'' +
                    ", partition=" + partition +
                    ", offset=" + offset +
                    ", numFails=" + numFails +
                    '}';
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

        //TODO Delete
        public static class OffsetComparator implements Comparator<Long> {
            public int compare(Long l1, Long l2) {
                return l1.compareTo(l2);
            }
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
