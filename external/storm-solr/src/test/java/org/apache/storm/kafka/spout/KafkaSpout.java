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
import org.apache.storm.kafka.spout.strategy.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.strategy.KafkaStream;
import org.apache.storm.kafka.spout.strategy.KafkaTupleBuilder;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class KafkaSpout<K,V> extends BaseRichSpout {
    private static final Logger log = LoggerFactory.getLogger(KafkaSpout.class);

    // Storm
    private Map conf;
    private TopologyContext context;
    protected SpoutOutputCollector collector;

    // Kafka
    private final KafkaSpoutConfig<K, V> kafkaSpoutConfig;
    private KafkaConsumer<K, V> kafkaConsumer;

    // Bookkeeping
    private OffsetsManager offsetsManager;
    Map<MessageId, Values> emittedTuples;           // Keeps a list of emitted tuples that are pending being acked
    private KafkaStream<K,V> kafkaStream;
    private ScheduledExecutorService offsetsCommitTimer;
    private KafkaTupleBuilder<K,V> kafkaTupleBuilder;

    public KafkaSpout(KafkaSpoutConfig<K,V> kafkaSpoutConfig) {
        this.kafkaSpoutConfig = kafkaSpoutConfig;                 // Pass in configuration
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.conf = conf;
        this.context = context;
        this.collector = collector;
        kafkaConsumer = new KafkaConsumer<>(kafkaSpoutConfig.getKafkaProps(), kafkaSpoutConfig.getKeyDeserializer(), kafkaSpoutConfig.getValueDeserializer());
        offsetsManager = new OffsetsManager();
        emittedTuples = new HashMap<>();

      /*  List<String> topics = new ArrayList<>();    // TODO
        subscribe(kafkaConsumer);
        kafkaConsumer.subscribe(topics);*/

        setOffsetsCommitTask();
    }

    /***/
    private void setOffsetsCommitTask() {
        offsetsCommitTimer = Executors.newSingleThreadScheduledExecutor();
        offsetsCommitTimer.schedule(new Runnable() {
            @Override
            public void run() {
                commitAckedRecords();
            }
        }, 100, TimeUnit.MILLISECONDS);
    }

    //TODO HANDLE PARALLELISM
//    String topologyMaxSpoutPending = Config.TOPOLOGY_MAX_SPOUT_PENDING;
    @Override
    public void nextTuple() {
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
        ConsumerRecords<K, V> consumerRecords = kafkaConsumer.poll(kafkaSpoutConfig.getPollTimeout());

        log.debug("Polled {[]} records from Kafka", consumerRecords.count());

        for (TopicPartition tp : consumerRecords.partitions()) {
            final Iterable<ConsumerRecord<K, V>> records = consumerRecords.records(tp.topic());     // TODO Decide if emmit per topic or per partition
            for (ConsumerRecord<K, V> record : records) {
                final Values tuple = kafkaTupleBuilder.buildTuple(tp, record);
                final MessageId messageId = createMessageId(record);            //TODO don't create message for
                collector.emit(getStreamId(), tuple, messageId);          // emits one tuple per record
                emittedTuples.put(messageId, tuple);
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

    private MessageId createMessageId(ConsumerRecord<K,V> consumerRecord) {
        return new MessageId(consumerRecord);
    }
    private String getStreamId() {
        return kafkaStream.getStreamId();
    }

    private final Lock ackedLock = new ReentrantLock();

    private class OffsetsManager {
        Map<TopicPartition, OffsetEntry> acked = new HashMap<>();
        final Map<TopicPartition, Set<MessageId>> failed = new HashMap<>();

        public void ack(MessageId msgId) {
            final TopicPartition tp = msgId.getTopicPartition();

            ackedLock.lock();
            try {
                if (!acked.containsKey(tp)) {
                    acked.put(tp, new OffsetEntry(null, null));
                }

                final OffsetEntry offsetEntry = acked.get(tp);
                offsetEntry.insert(msgId);
            } finally {
                ackedLock.unlock();
            }

            // Removed acked tuples from the emittedTuples data structure
            emittedTuples.remove(msgId);

            // if acked message is a retry, remove it from failed data structure
            if (failed.containsKey(tp)) {
                final Set<MessageId> msgIds = failed.get(tp);
                msgIds.remove(msgId);
                if (msgIds.isEmpty()) {
                    failed.remove(tp);
                }
            }
        }

        public void fail(MessageId msgId) {
            final TopicPartition tp = msgId.getTopicPartition();
            if (!failed.containsKey(tp)) {
                failed.put(tp, new HashSet<MessageId>());
            }

            final Set<MessageId> msgIds = failed.get(tp);
            msgId.incrementNumFails();        // increment number of failures counter
            msgIds.add(msgId);

            // limit to max number of retries
            if (msgId.numFails >= maxRetries()) {
                log.debug("Reached the maximum number of retries. Adding message {[]} to list of messages to be committed to kafka", msgId);
                ack(msgId);
                msgIds.remove(msgId);
                if (msgIds.isEmpty()) {
                    failed.remove(tp);
                }
            }
        }


        public boolean retry() {
            return failed.size() > 0;
        }

        public void retryFailed() {
            for (TopicPartition tp: failed.keySet()) {
                for (MessageId msgId : failed.get(tp)) {
                    Values tuple = emittedTuples.get(msgId);
                    log.debug("Retrying tuple. [msgId={}, tuple={}]", msgId, tuple);
                    collector.emit(getStreamId(tp), tuple, msgId);
                }
            }
        }

        /** Commits to kafka the maximum sequence of continuous offsets that have been acked for a partition */
        public void commitAckedOffsets() {
            final Map<TopicPartition, OffsetAndMetadata> ackedOffsets = new HashMap<>();
            for (TopicPartition tp : acked.keySet()) {
                final MessageId msgId = acked.get(tp).getMaxOffsetMsgAcked();
                ackedOffsets.put(tp, new OffsetAndMetadata(msgId.offset, msgId.metadata(Thread.currentThread())));
            }

            kafkaConsumer.commitSync(ackedOffsets);
            log.debug("Offsets successfully committed to Kafka {[]}", ackedOffsets);

            // All acked offsets have been committed, so clean data structure
            acked = new HashMap<>();
        }

        // TODO
        private int maxRetries() {
            return Integer.MAX_VALUE;
        }
    }

    private final class OffsetEntry {
        private long lastCommittedOffset = 0;
        private List<MessageId> offsets = new ArrayList<>();      // in root keep only two offsets - first and last
        private OffsetEntry prev;
        private OffsetEntry next;

        public OffsetEntry(OffsetEntry prev, OffsetEntry next) {
            this.prev = prev;
            this.next = next;
        }

        public void insert(MessageId msgId) {
            insert(msgId, this);
            merge();
        }

        private void merge() {
            while (this.next != null && (this.getLastOffset() - this.next.getFirstOffset()) == 1) {
                offsets.addAll(this.next.offsets);
                deleteEntry(this.next);
            }
        }

        private void deleteEntry(OffsetEntry entry) {
            if (entry.prev != null) {
                entry.prev.next = entry.next;
            }
            if (entry.next != null) {
                entry.next.prev = entry.prev;
            }
            entry.prev = null;
            entry.next = null;
        }


        //TODO: Make it Iterative to be faster
        private void insert(MessageId msgId, OffsetEntry offsetEntry) {
            if (offsetEntry == null) {
                return;
            }

            if (offsets.isEmpty() || msgId.offset == (getLastOffset() + 1)) {           // msgId becomes last element of this offsets sublist
                setLast(msgId);
            } else if (msgId.offset == (getFirstOffset() - 1)) {   // msgId becomes first element of this offsets sublist
                setFirst(msgId);
            } else if (msgId.offset < getFirstOffset()) {           // insert a new OffsetEntry element in the list
                OffsetEntry newEntry = new OffsetEntry(this.prev, this);
                this.prev.next = newEntry;
                this.prev = newEntry;
                newEntry.setFirst(msgId);
                return;
                // insert and return
            }

            insert(msgId, offsetEntry.next);
        }

        public MessageId getMaxOffsetMsgAcked() {       //TODO Rename this method
            MessageId msgId = null;
            if (isHead() && !offsets.isEmpty() && offsets.get(0).offset == lastCommittedOffset + 1) {
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

        private boolean isHead() {
            return prev == null;
        }

        @Override
        public String toString() {
            return "{" +
                    "lastCommittedOffset=" + lastCommittedOffset +
                    ", offsets=" + offsets +
                    ", prev=" + prev +
                    ", next=" + next +
                    '}';
        }

        //TODO for debug
        void printAllLevels() {
            printAllLevels(this);
        }

        private void printAllLevels(OffsetEntry head) {
            while (head != null) {
                log.debug(head.toString());
            }
        }
    }





    private void serialize() {

    }



    private static class MessageId {
        TopicPartition topicPart;
        private long offset;
        int numFails = 0;

        public MessageId(ConsumerRecord consumerRecord) {
            this(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()), consumerRecord.offset());
        }

        public MessageId(TopicPartition topicPart, long offset) {
            this.topicPart = topicPart;
            this.offset = offset;
        }

        public void incrementNumFails() {
            ++numFails;
        }

        public int partition() {
            return topicPart.partition();
        }

        public String topic() {
            return topicPart.topic();
        }

        public TopicPartition getTopicPartition() {
            return topicPart;
        }

        public String metadata(Thread currThread) {
            return "{" +
                    "topic='" + topic() + '\'' +
                    ", partition=" + partition() +
                    ", offset=" + offset +
                    ", numFails=" + numFails +
                    ", thread='" + currThread.getName() + "'" +
                    '}';
        }

        @Override
        public String toString() {
            return "MessageId{" +
                    "topic='" + topic() + '\'' +
                    ", partition=" + partition() +
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
            MessageId messageId = (MessageId) o;
            if (offset != messageId.offset) {
                return false;
            }
            return topicPart.equals(messageId.topicPart);
        }

        @Override
        public int hashCode() {
            int result = topicPart.hashCode();
            result = 31 * result + (int) (offset ^ (offset >>> 32));
            return result;
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
        return new Fields();
    }   TODO

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
