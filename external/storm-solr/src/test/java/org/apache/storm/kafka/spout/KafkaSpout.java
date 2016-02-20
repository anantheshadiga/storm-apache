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
import org.apache.storm.kafka.spout.strategy.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.strategy.KafkaSpoutStream;
import org.apache.storm.kafka.spout.strategy.KafkaTupleBuilder;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class KafkaSpout<K,V> extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSpout.class);
    private static final Comparator<MessageId> OFFSET_COMPARATOR = new OffsetComparator();

    private volatile Lock ackedLock;

    //TODO Manage leaked connections
    // Storm
    private Map conf;
    private TopologyContext context;
    protected SpoutOutputCollector collector;

    // Kafka
    private final KafkaSpoutConfig<K, V> kafkaSpoutConfig;
    private KafkaConsumer<K, V> kafkaConsumer;

    // Bookkeeping
    private ScheduledExecutorService commitOffsetsTask;
    private IOffsetsManager offsetsManager;
    private KafkaSpoutStream kafkaStream;
    private KafkaTupleBuilder<K,V> kafkaTupleBuilder;


    private transient Map<MessageId, Values> emittedTuples;           // Keeps a list of emitted tuples that are pending being acked or failed
    private transient Map<TopicPartition, Set<MessageId>> failed;     // failed tuples. They stay in this list until success or max retries is reached
    private transient Map<TopicPartition, OffsetEntry> acked;         // emitted tuples that were successfully acked. These tuples will be committed by the commitOffsetsTask
    private int maxRetries;


    public KafkaSpout(KafkaSpoutConfig<K,V> kafkaSpoutConfig) {
        this.kafkaSpoutConfig = kafkaSpoutConfig;                 // Pass in configuration
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        // Spout internals
        this.conf = conf;
        this.context = context;
        this.collector = collector;

        // Bookkeeping
        emittedTuples = new HashMap<>();
        failed = new HashMap<>();
        acked = new HashMap<>();

        // Kafka consumer
        kafkaConsumer = new KafkaConsumer<>(kafkaSpoutConfig.getKafkaProps(),
                kafkaSpoutConfig.getKeyDeserializer(), kafkaSpoutConfig.getValueDeserializer());

        kafkaConsumer.subscribe(kafkaSpoutConfig.getTopics(), new KafkaSpoutConsumerRebalanceListener());

        if (!kafkaSpoutConfig.isAutoCommitMode()) {     // If it is auto commit, no need to commit offsets manually
            createCommitOffsetsTask();
        }

        maxRetries = kafkaSpoutConfig.getMaxRetries()

        ackedLock = new ReentrantLock();
    }

    private class KafkaSpoutConsumerRebalanceListener implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            commitAckedRecords();  // commit acked records,
            clearFailedTuples();   // remove all failed tuples form list to avoid duplication
            //TODO Racing condition when rebalance happens
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            LOG.debug("Partition reassignment occurred. [consumer-group={}, consumer={}, topic-partitions={}]",
                    kafkaSpoutConfig.getConsumerGroupId(), kafkaConsumer, partitions);
        }
    }

    private void clearFailedTuples() {
        failed = new HashMap<>();
    }


    private void createCommitOffsetsTask() {
        commitOffsetsTask = Executors.newSingleThreadScheduledExecutor();
        commitOffsetsTask.schedule(new Runnable() {
            @Override
            public void run() {
                commitAckedRecords();
            }
        }, kafkaSpoutConfig.getCommitFreqMs(), TimeUnit.MILLISECONDS);
    }

    //TODO HANDLE PARALLELISM
//    String topologyMaxSpoutPending = Config.TOPOLOGY_MAX_SPOUT_PENDING;
    @Override
    public void nextTuple() {
        if (retry()) {              // Don't process new tuples until the failed tuples have all been acked
            retryFailedTuples();
        } else {
            emitTuples(poll());
        }
    }

    private ConsumerRecords<K, V> poll() {
        final ConsumerRecords<K, V> consumerRecords = kafkaConsumer.poll(kafkaSpoutConfig.getPollTimeoutMs());
        LOG.debug("Polled {[]} records from Kafka", consumerRecords.count());
        return consumerRecords;
    }

    private void emitTuples(ConsumerRecords<K, V> consumerRecords) {
        for (TopicPartition tp : consumerRecords.partitions()) {
            final Iterable<ConsumerRecord<K, V>> records = consumerRecords.records(tp.topic());     // TODO Decide if emmit per topic or per partition
            for (ConsumerRecord<K, V> record : records) {
                final Values tuple = kafkaTupleBuilder.buildTuple(tp, record);
                final MessageId messageId = new MessageId(record);                   // TODO don't create message for non acking mode?
                collector.emit(kafkaStream.getStreamId(), tuple, messageId);           // emits one tuple per record
                emittedTuples.put(messageId, tuple);
            }
        }
    }

    private boolean retry() {
        return failed.size() > 0;
    }

    private void retryFailedTuples() {
        for (TopicPartition tp : failed.keySet()) {
            for (MessageId msgId : failed.get(tp)) {
                final Values tuple = emittedTuples.get(msgId);
                LOG.debug("Retrying tuple. [msgId={}, tuple={}]", msgId, tuple);
                collector.emit(kafkaStream.getStreamId(), tuple, msgId);
            }
        }
    }

    private void commitAckedRecords() {
        final Map<TopicPartition, OffsetAndMetadata> toCommitOffsets = new HashMap<>();

        LOG.debug("Committing acked offsets to Kafka");
        // lock because ack and commit happen in different threads
        ackedLock.lock();
        try {
            for (TopicPartition tp : acked.keySet()) {
                OffsetEntry offsetEntry = acked.get(tp);
                OffsetAndMetadata offsetAndMetadata = offsetEntry.findOffsetToCommit();
                if (offsetAndMetadata != null) {
                    toCommitOffsets.put(tp, offsetAndMetadata);
                }
            }
        } finally {
            ackedLock.unlock();
        }

        kafkaConsumer.commitSync(toCommitOffsets);
        LOG.debug("Offsets successfully committed to Kafka {[]}", toCommitOffsets);

        // Instead of iterating again, the other option would be to commit each TopicPartition prior loop,
        // but the multiple networks calls should be more expensive than iteration twice over a small loop
        ackedLock.lock();
        try {
            for (TopicPartition tp : acked.keySet()) {
                OffsetEntry offsetEntry = acked.get(tp);
                offsetEntry.updateState(toCommitOffsets.get(tp));
                updateState(tp, offsetEntry);
            }
        } finally {
            ackedLock.unlock();
        }
    }

    private void updateState(TopicPartition tp, OffsetEntry offsetEntry) {
        if (offsetEntry.isEmpty()) {
            acked.remove(tp);
        }
    }

    @Override
    public void ack(Object messageId) {
        final MessageId msgId = (MessageId) messageId;
        final TopicPartition tp = msgId.getTopicPartition();

        // lock because ack and commit happen in different threads
        ackedLock.lock();
        try {
            if (!acked.containsKey(tp)) {
                acked.put(tp, new OffsetEntry());
            }
            final OffsetEntry offsetEntry = acked.get(tp);
            offsetEntry.add(msgId);
        } finally {
            ackedLock.unlock();
        }

        // Removed acked tuples from the emittedTuples data structure
        emittedTuples.remove(msgId);

        // if ackedMsgs message is a retry, remove it from failed data structure
        if (failed.containsKey(tp)) {
            final Set<MessageId> msgIds = failed.get(tp);
            msgIds.remove(msgId);
            if (msgIds.isEmpty()) {
                failed.remove(tp);
            }
        }
    }

    //TODO: HANDLE CONSUMER REBALANCE

    @Override
    public void fail(Object messageId) {
        final MessageId msgId = (MessageId) messageId;
        final TopicPartition tp = msgId.getTopicPartition();
        if (!failed.containsKey(tp)) {
            failed.put(tp, new HashSet<MessageId>());
        }

        final Set<MessageId> msgIds = failed.get(tp);
        if (msgIds.contains(msgId)) {
            msgIds.remove(msgId);
        } else {
            msgId.incrementNumFails();        // increment number of failures counter
            msgIds.add(msgId);
        }

        // limit to max number of retries
        if (msgId.numFails() >= maxRetries) {
            LOG.debug("Reached the maximum number of retries. Adding {[]} to list of messages to be committed to kafka", msgId);
            ack(msgId);
            msgIds.remove(msgId);
            if (msgIds.isEmpty()) {
                failed.remove(tp);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(kafkaStream.getStreamId(), kafkaStream.getOutputFields());
    }

    @Override
    public void activate() {
        //resume processing
    }

    @Override
    public void deactivate() {
        kafkaConsumer.pause();
        //commit
    }

    @Override
    public void close() {
        try {
            kafkaConsumer.wakeup();
            commitAckedRecords();
        } finally {
            //remove resources
            kafkaConsumer.close();
        }
    }

    // ======= Offsets Commit Management ==========

    private static class OffsetComparator implements Comparator<MessageId> {
        public int compare(MessageId m1, MessageId m2) {
            return m1.offset() < m2.offset() ? -1 : m1.offset() == m2.offset() ? 0 : 1;
        }
    }

    private class OffsetEntry {
        private long committedOffset = -1;          // last offset committed to Kafka
        private long toCommitOffset;                // last offset to be committed in the next commit operation
        private final Set<MessageId> ackedMsgs = new TreeSet<>(OFFSET_COMPARATOR);     // sort messages by ascending order of offset
        private Set<MessageId> toCommitMsgs = new TreeSet<>(OFFSET_COMPARATOR);        // Messages that contain the offsets to be committed in the next commit operation

        public void add(MessageId msgId) {          // O(Log N)
            ackedMsgs.add(msgId);
        }

        /**
         * This method has side effects. The method updateState should be called after this method.
         */
        public OffsetAndMetadata findOffsetToCommit() {
            long currOffset;
            OffsetAndMetadata offsetAndMetadata = null;
            toCommitMsgs = new TreeSet<>(OFFSET_COMPARATOR);
            toCommitOffset = committedOffset;
            MessageId toCommitMsg = null;

            for (MessageId ackedMsg : ackedMsgs) {  // for K matching messages complexity is K*(Log*N). K <= N
                if ((currOffset = ackedMsg.offset()) == toCommitOffset + 1) {    // found the next offset to commit
                    toCommitMsgs.add(ackedMsg);
                    toCommitMsg = ackedMsg;
                    toCommitOffset = currOffset;
                } else if (ackedMsg.offset() > toCommitOffset + 1) {    // offset found is not continuous to the offsets listed to go in the next commit, so stop search
                    break;
                } else {
                    LOG.debug("Unexpected offset found {[]}. {}", ackedMsg.offset(), toString());
                    break;
                }
            }

            if (!toCommitMsgs.isEmpty()) {
                offsetAndMetadata = new OffsetAndMetadata(toCommitOffset, toCommitMsg.getMetadata(Thread.currentThread()));
                LOG.debug("Last offset to be committed in the next commit call: {[]}", offsetAndMetadata.offset());
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
        public void updateState(OffsetAndMetadata offsetAndMetadata) {
            committedOffset = offsetAndMetadata.offset();
            toCommitMsgs = new TreeSet<>(OFFSET_COMPARATOR);
            ackedMsgs.removeAll(toCommitMsgs);
        }

        public boolean isEmpty() {
            return ackedMsgs.isEmpty();
        }

        @Override
        public String toString() {
            return "OffsetEntry{" +
                    "committedOffset=" + committedOffset +
                    ", toCommitOffset=" + toCommitOffset +
                    ", ackedMsgs=" + ackedMsgs +
                    ", toCommitMsgs=" + toCommitMsgs +
                    '}';
        }
    }


}
