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

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class KafkaSpout<K,V> extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSpout.class);
    private static final Comparator<MessageId> OFFSET_COMPARATOR = new OffsetComparator();

    // Storm
    private Map conf;
    private TopologyContext context;
    protected SpoutOutputCollector collector;

    // Kafka
    private final KafkaSpoutConfig<K, V> kafkaSpoutConfig;
    private KafkaConsumer<K, V> kafkaConsumer;

    // Bookkeeping
    private KafkaSpoutStream kafkaSpoutStream;
    private KafkaTupleBuilder<K,V> tupleBuilder;
    private transient ScheduledExecutorService commitOffsetsTask;
    private transient Lock ackCommitLock;
    private transient volatile boolean commit;
    private transient Map<MessageId, Values> emittedTuples;           // Keeps a list of emitted tuples that are pending being acked or failed
    private transient Map<TopicPartition, Set<MessageId>> failed;     // failed tuples. They stay in this list until success or max retries is reached
    private transient Map<TopicPartition, OffsetEntry> acked;         // emitted tuples that were successfully acked. These tuples will be committed by the commitOffsetsTask or on consumer rebalance
    private transient Set<MessageId> blackList;                       // all the tuples that are in traffic when the rebalance occurs will be added to black list to be disregarded when they are either acked or failed
    private transient int maxRetries;

    public KafkaSpout(KafkaSpoutConfig<K,V> kafkaSpoutConfig, KafkaSpoutStream kafkaSpoutStream, KafkaTupleBuilder<K,V> tupleBuilder) {
        this.kafkaSpoutConfig = kafkaSpoutConfig;                 // Pass in configuration
        this.kafkaSpoutStream = kafkaSpoutStream;
        this.tupleBuilder = tupleBuilder;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        // Spout internals
        this.conf = conf;
        this.context = context;
        this.collector = collector;

        // Bookkeeping objects
        emittedTuples = new HashMap<>();
        failed = new HashMap<>();
        acked = new HashMap<>();
        blackList = new HashSet<>();
        ackCommitLock = new ReentrantLock();
        maxRetries = kafkaSpoutConfig.getMaxTupleRetries();

        // Kafka consumer
        kafkaConsumer = new KafkaConsumer<>(kafkaSpoutConfig.getKafkaProps(),
                kafkaSpoutConfig.getKeyDeserializer(), kafkaSpoutConfig.getValueDeserializer());
        kafkaConsumer.subscribe(kafkaSpoutConfig.getSubscribedTopics(), new KafkaSpoutConsumerRebalanceListener());

        // Create commit offsets task
        if (!kafkaSpoutConfig.isConsumerAutoCommitMode()) {     // If it is auto commit, no need to commit offsets manually
            createCommitOffsetsTask();
        }
    }

    // ======== Commit Offsets Task =======

    private void createCommitOffsetsTask() {
        commitOffsetsTask = Executors.newSingleThreadScheduledExecutor(commitOffsetsThreadFactory());
        commitOffsetsTask.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                commit = true;
            }
        }, 1000, kafkaSpoutConfig.getOffsetsCommitFreqMs(), TimeUnit.MILLISECONDS);
    }

    private ThreadFactory commitOffsetsThreadFactory() {
        return new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "kafka-spout-commit-offsets-thread");
            }
        };
    }

    // ======== Next Tuple =======

    @Override
    public void nextTuple() {
        if(commit) {
            commitAckedTuples();
        } else if (retry()) {              // Don't process new tuples until the failed tuples have all been acked
            retryFailedTuples();
        } else {
            emitTuples(poll());
        }
    }

    private ConsumerRecords<K, V> poll() {
        final ConsumerRecords<K, V> consumerRecords = kafkaConsumer.poll(kafkaSpoutConfig.getPollTimeoutMs());
        LOG.debug("Polled [{}] records from Kafka", consumerRecords.count());
        return consumerRecords;
    }

    private void emitTuples(ConsumerRecords<K, V> consumerRecords) {
        for (TopicPartition tp : consumerRecords.partitions()) {
            final Iterable<ConsumerRecord<K, V>> records = consumerRecords.records(tp.topic());     // TODO Decide if want to give flexibility to emmit/poll either per topic or per partition
            for (ConsumerRecord<K, V> record : records) {
                final Values tuple = tupleBuilder.buildTuple(record);
                final MessageId messageId = new MessageId(record);                                  // TODO don't create message for non acking mode. Should we support non acking mode?
                collector.emit(kafkaSpoutStream.getStreamId(), tuple, messageId);           // emits one tuple per record
                emittedTuples.put(messageId, tuple);
                LOG.debug("Emitted tuple for record {}", record);
            }
        }
    }

    private boolean retry() {
        return failed.size() > 0;
    }

    private void retryFailedTuples() {
        for (TopicPartition tp : failed.keySet()) {
            for (MessageId msgId : failed.get(tp)) {
                if (isInBlackList(msgId)) {
                    removeFromBlacklist(msgId);
                    removeFromFailed(tp, msgId);
                } else {
                    final Values tuple = emittedTuples.get(msgId);
                    LOG.debug("Retrying tuple. [msgId={}, tuple={}]", msgId, tuple);
                    collector.emit(kafkaSpoutStream.getStreamId(), tuple, msgId);
                }
            }
        }
    }

    // all the tuples that are in traffic when the rebalance occurs will be added
    // to black list to be disregarded when they are either acked or failed
    private boolean isInBlackList(MessageId msgId) {
        return blackList.contains(msgId);
    }

    private void removeFromBlacklist(MessageId msgId) {
        blackList.remove(msgId);
    }

    // ======== Ack =======

    @Override
    public void ack(Object messageId) {
        final MessageId msgId = (MessageId) messageId;
        final TopicPartition tp = msgId.getTopicPartition();

        if (isInBlackList(msgId)) {
            removeFromBlacklist(msgId);
        } else {
            addAckedTuples(tp, msgId);
            // Removed acked tuples from the emittedTuples data structure
            emittedTuples.remove(msgId);
            // if this acked msg is a retry, remove it from failed data structure
            removeFromFailed(tp, msgId);
        }
    }

    private void addAckedTuples(TopicPartition tp, MessageId msgId) {
        // lock because ack and commit happen in different threads
        ackCommitLock.lock();
        try {
            if (!acked.containsKey(tp)) {
                acked.put(tp, new OffsetEntry(tp));
            }
            acked.get(tp).add(msgId);
        } finally {
            ackCommitLock.unlock();
        }
    }

    // ======== Fail =======

    @Override
    public void fail(Object messageId) {
        final MessageId msgId = (MessageId) messageId;

        if (isInBlackList(msgId)) {
            removeFromBlacklist(msgId);
        } else {
            final TopicPartition tp = msgId.getTopicPartition();
            // limit to max number of retries
            if (msgId.numFails() >= maxRetries) {
                LOG.debug("Reached the maximum number of retries. Adding [{]} to list of messages to be committed to kafka", msgId);
                ack(msgId);
                removeFromFailed(tp, msgId);
            } else {
                addToFailed(tp, msgId);
            }
        }
    }

    private void addToFailed(TopicPartition tp, MessageId msgId) {
        if (!failed.containsKey(tp)) {
            failed.put(tp, new HashSet<MessageId>());
        }
        final Set<MessageId> msgIds = failed.get(tp);
        if (msgIds.contains(msgId)) {      // do this to update the counter of the message
            msgIds.remove(msgId);
        }
        msgId.incrementNumFails();         // increment number of failures counter
        msgIds.add(msgId);
    }

    private void removeFromFailed(TopicPartition tp, MessageId msgId) {
        if (failed.containsKey(tp)) {
            final Set<MessageId> msgIds = failed.get(tp);
            msgIds.remove(msgId);
            if (msgIds.isEmpty()) {
                failed.remove(tp);
            }
            LOG.debug("Removing from failed list [{}, {}]", tp, msgId);
        }
    }

    // ======== Activate / Deactivate =======

    @Override
    public void activate() {
        // Shouldn't have to do anything for now. If specific cases need to be handled logic will go here
    }

    @Override
    public void deactivate() {
        commitAckedTuples();
    }

    @Override
    public void close() {
        try {
            kafkaConsumer.wakeup();
            commitAckedTuples();
        } finally {
            //remove resources
            kafkaConsumer.close();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(kafkaSpoutStream.getStreamId(), kafkaSpoutStream.getOutputFields());
    }

    // ====== Private helper methods ======

    private void commitAckedTuples() {
        final Map<TopicPartition, OffsetAndMetadata> toCommitOffsets = new HashMap<>();

        // lock because ack and commit happen in different threads
        ackCommitLock.lock();
        try {
            for (TopicPartition tp : acked.keySet()) {
                final OffsetEntry offsetEntry = acked.get(tp);
                OffsetAndMetadata offsetAndMetadata = offsetEntry.findOffsetToCommit();
                if (offsetAndMetadata != null) {
                    toCommitOffsets.put(tp, offsetAndMetadata);
                }
            }

            if (!toCommitOffsets.isEmpty()) {
                kafkaConsumer.commitSync(toCommitOffsets);
                LOG.debug("Offsets successfully committed to Kafka [{]}", toCommitOffsets);
                // Instead of iterating again, we could commit each TopicPartition in the prior loop,
                // but the multiple networks calls should be more expensive than iterating twice over a small loop
                for (TopicPartition tp : acked.keySet()) {
                    OffsetEntry offsetEntry = acked.get(tp);
                    offsetEntry.updateAckedState(toCommitOffsets.get(tp));
                    updateAckedState(tp, offsetEntry);
                }
            } else {
                LOG.trace("No offsets to commit. {}", toString());
            }

        } catch (Exception e) {
            LOG.error("Exception occurred while committing acked tuples offsets to Kafka", e);
        } finally {
            commit = false;
            ackCommitLock.unlock();
        }
    }

    private void updateAckedState(TopicPartition tp, OffsetEntry offsetEntry) {
        // lock because ack and commit happen in different threads
        ackCommitLock.lock();
        try {
            if (offsetEntry.isEmpty()) {
                acked.remove(tp);
            }
        } finally {
            ackCommitLock.unlock();
        }
    }

    @Override
    public String toString() {
        return "KafkaSpout{" +
                "emittedTuples=" + emittedTuples +
                ", failed=" + failed +
                ", acked=" + acked +
                ", blackList=" + blackList +
                ", commit=" + commit +
                "} ";
    }

    // ======= Offsets Commit Management ==========

    private static class OffsetComparator implements Comparator<MessageId> {
        public int compare(MessageId m1, MessageId m2) {
            return m1.offset() < m2.offset() ? -1 : m1.offset() == m2.offset() ? 0 : 1;
        }
    }

    /** This class is not thread safe */
    // Although this class is called by multiple (2) threads, all the calling methods are properly synchronized
    private class OffsetEntry {
        private long committedOffset;               // last offset committed to Kafka
        private long toCommitOffset;                // last offset to be committed in the next commit operation
        private final Set<MessageId> ackedMsgs = new TreeSet<>(OFFSET_COMPARATOR);     // sort messages by ascending order of offset
        private Set<MessageId> toCommitMsgs = new TreeSet<>(OFFSET_COMPARATOR);        // Messages that contain the offsets to be committed in the next commit operation
        private TopicPartition tp;

        public OffsetEntry(TopicPartition tp) {
            this.tp = tp;
            OffsetAndMetadata committed = kafkaConsumer.committed(tp);
            committedOffset = committed == null ? -1 : committed.offset();
            LOG.debug("Created OffsetEntry for [topic-partition={}, last-committed-offset={}]", tp, committedOffset);
        }

        public void add(MessageId msgId) {          // O(Log N)
            ackedMsgs.add(msgId);
        }

        /**
         * This method has side effects. The method updateAckedState should be called after this method.
         */
        public OffsetAndMetadata findOffsetToCommit() {
            long currOffset;
            OffsetAndMetadata offsetAndMetadata = null;
            toCommitMsgs = new TreeSet<>(OFFSET_COMPARATOR);
            toCommitOffset = committedOffset;
            MessageId toCommitMsg = null;

            for (MessageId ackedMsg : ackedMsgs) {  // for K matching messages complexity is K*(Log*N). K <= N
                if ((currOffset = ackedMsg.offset()) != toCommitOffset) {
                    if (currOffset == toCommitOffset + 1) {    // found the next offset to commit
                        toCommitMsgs.add(ackedMsg);
                        toCommitMsg = ackedMsg;
                        toCommitOffset = currOffset;
                    } else if (ackedMsg.offset() > toCommitOffset + 1) {    // offset found is not continuous to the offsets listed to go in the next commit, so stop search
                        break;
                    } else {
                        LOG.debug("Unexpected offset found [{}]. {}", ackedMsg.offset(), toString());
                        break;
                    }
                }
            }

            if (!toCommitMsgs.isEmpty()) {
                offsetAndMetadata = new OffsetAndMetadata(toCommitOffset, toCommitMsg.getMetadata(Thread.currentThread()));
                LOG.debug("Last offset to be committed in the next commit call: [{}]", offsetAndMetadata.offset());
                LOG.trace(toString());
            } else {
                LOG.debug("No offsets found ready to commit");
                LOG.trace(toString());
            }
            return offsetAndMetadata;
        }

        /**
         * This method has side effects and should be called after findOffsetToCommit
         */
        public void updateAckedState(OffsetAndMetadata offsetAndMetadata) {
            if (offsetAndMetadata != null) {
                committedOffset = offsetAndMetadata.offset();
                toCommitMsgs = new TreeSet<>(OFFSET_COMPARATOR);
                ackedMsgs.removeAll(toCommitMsgs);
            }
        }

        public boolean isEmpty() {
            return ackedMsgs.isEmpty();
        }

        @Override
        public String toString() {
            return "OffsetEntry{" +
                    "topic-partition=" + tp +
                    ", committedOffset=" + committedOffset +
                    ", toCommitOffset=" + toCommitOffset +
                    ", ackedMsgs=" + ackedMsgs +
                    ", toCommitMsgs=" + toCommitMsgs +
                    '}';
        }
    }

    // =========== Consumer Rebalance Listener - On the same thread as the caller ===========

    private class KafkaSpoutConsumerRebalanceListener implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            LOG.debug("Partitions revoked. [consumer-group={}, consumer={}, topic-partitions={}]",
                    kafkaSpoutConfig.getConsumerGroupId(), kafkaConsumer, partitions);
            commitAckedTuples();  // commit acked records,
            copyEmittedTuplesToBlackList(); // all the tuples that are in traffic when the rebalance occurs will be added to black list to avoid duplication
            clearFailed();   // remove all failed tuples form list to avoid duplication
            clearEmittedTuples();    // clear emitted tuples
        }

        private void copyEmittedTuplesToBlackList() {
            blackList.addAll(emittedTuples.keySet());
        }

        private void clearFailed() {
            failed = new HashMap<>();
        }

        private void clearEmittedTuples() {
            emittedTuples = new HashMap<>();
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            LOG.debug("Partitions reassignment. [consumer-group={}, consumer={}, topic-partitions={}]",
                    kafkaSpoutConfig.getConsumerGroupId(), kafkaConsumer, partitions);
        }
    }
}
