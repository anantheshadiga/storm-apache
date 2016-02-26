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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class KafkaSpout<K,V> extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSpout.class);
    private static final Comparator<MessageId> OFFSET_COMPARATOR = new OffsetComparator();

    // Storm
    protected SpoutOutputCollector collector;

    // Kafka
    private final KafkaSpoutConfig<K, V> kafkaSpoutConfig;
    private KafkaConsumer<K, V> kafkaConsumer;

    // Bookkeeping
    private KafkaSpoutStream kafkaSpoutStream;
    private KafkaTupleBuilder<K,V> tupleBuilder;
    private transient Timer timer;                                    // timer == null for auto commit mode
    private transient ScheduledExecutorService commitOffsetsTask;
    private transient Map<TopicPartition, OffsetEntry> acked;         // emitted tuples that were successfully acked. These tuples will be committed by the commitOffsetsTask or on consumer rebalance
    private transient int maxRetries;

    public KafkaSpout(KafkaSpoutConfig<K,V> kafkaSpoutConfig, KafkaSpoutStream kafkaSpoutStream, KafkaTupleBuilder<K,V> tupleBuilder) {
        this.kafkaSpoutConfig = kafkaSpoutConfig;                 // Pass in configuration
        this.kafkaSpoutStream = kafkaSpoutStream;
        this.tupleBuilder = tupleBuilder;
    }

    class Timer {
        private final long frequency;
        private final long delay;
        private final TimeUnit timeUnit;
        private final long frequencyNanos;
        private long start;

        /** Creates a timer to expire at the given frequency and starting with the specified time delay */
        public Timer(long frequency, long delay, TimeUnit timeUnit) {
            this.frequency = frequency;
            this.delay = delay;
            this.timeUnit = timeUnit;

            frequencyNanos = timeUnit.toNanos(frequency);
            start = System.nanoTime() + timeUnit.toNanos(delay);
        }

        public long frequency() {
            return frequency;
        }

        public long delay() {
            return delay;
        }

        public TimeUnit getTimeUnit() {
            return timeUnit;
        }

        public boolean expired() {
            final boolean expired = System.nanoTime() - start > frequencyNanos;
            if (expired) {
                start = System.nanoTime();
            }
            return expired;
        }
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        // Spout internals
        this.collector = collector;

        // Bookkeeping objects
        acked = new HashMap<>();
        maxRetries = kafkaSpoutConfig.getMaxTupleRetries();

        // Kafka consumer
        kafkaConsumer = new KafkaConsumer<>(kafkaSpoutConfig.getKafkaProps(),
                kafkaSpoutConfig.getKeyDeserializer(), kafkaSpoutConfig.getValueDeserializer());
        kafkaConsumer.subscribe(kafkaSpoutConfig.getSubscribedTopics(), new KafkaSpoutConsumerRebalanceListener());

        // Create commit offsets timer

        if (!kafkaSpoutConfig.isConsumerAutoCommitMode()) {     // If it is auto commit, no need to commit offsets manually
            timer = new Timer(kafkaSpoutConfig.getOffsetsCommitFreqMs(), 500, TimeUnit.MILLISECONDS);
        }
    }

    // ======== Next Tuple =======

    @Override
    public void nextTuple() {
        if(commit()) {
            commitAckedTuples();
        } else {
            emitTuples(poll());
        }
    }

    private boolean commit() {
        return timer != null && timer.expired();
    }

    private ConsumerRecords<K, V> poll() {
        final ConsumerRecords<K, V> consumerRecords = kafkaConsumer.poll(kafkaSpoutConfig.getPollTimeoutMs());
        LOG.debug("Polled [{}] records from Kafka. [{}]", consumerRecords.count(), consumerRecords);
        return consumerRecords;
    }

    private void emitTuples(ConsumerRecords<K, V> consumerRecords) {
        for (TopicPartition tp : consumerRecords.partitions()) {
            final Iterable<ConsumerRecord<K, V>> records = consumerRecords.records(tp.topic());     // TODO Decide if want to give flexibility to emmit/poll either per topic or per partition
            for (ConsumerRecord<K, V> record : records) {
                final List<Object> tuple = tupleBuilder.buildTuple(record);
                final MessageId messageId = new MessageId(record, tuple);                                  // TODO don't create message for non acking mode. Should we support non acking mode?
                collector.emit(kafkaSpoutStream.getStreamId(), tuple, messageId);           // emits one tuple per record
                LOG.debug("Emitted tuple [{}] for record [{}]", tuple, record);
            }
        }
    }

    // ======== Ack =======
    @Override
    public void ack(Object messageId) {
        final MessageId msgId = (MessageId) messageId;
        final TopicPartition tp = msgId.getTopicPartition();

        if (!acked.containsKey(tp)) {
            acked.put(tp, new OffsetEntry(tp));
        }
        acked.get(tp).add(msgId);
        LOG.debug("Adding acked message to [{}] to list of messages to be committed to Kafka", msgId);
    }

    // ======== Fail =======

    @Override
    public void fail(Object messageId) {
        final MessageId msgId = (MessageId) messageId;
        if (msgId.numFails() < maxRetries) {
            msgId.incrementNumFails();
            collector.emit(kafkaSpoutStream.getStreamId(), msgId.getTuple(), messageId);
            LOG.debug("Retried tuple with message id [{}]", messageId);
        } else { // limit to max number of retries
            LOG.debug("Reached maximum number of retries. Message being marked as acked.");
            ack(msgId);
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
        try {
            for (Map.Entry<TopicPartition, OffsetEntry> tpOffset : acked.entrySet()) {
                final OffsetAndMetadata offsetAndMetadata = tpOffset.getValue().findOffsetToCommit();
                if (offsetAndMetadata != null) {
                    toCommitOffsets.put(tpOffset.getKey(), offsetAndMetadata);
                }
            }

            if (!toCommitOffsets.isEmpty()) {
                kafkaConsumer.commitSync(toCommitOffsets);
                LOG.debug("Offsets successfully committed to Kafka [{}]", toCommitOffsets);
                // Instead of iterating again, we could commit and update state for each TopicPartition in the prior loop,
                // but the multiple network calls should be more expensive than iterating twice over a small loop
                for (Map.Entry<TopicPartition, OffsetEntry> tpOffset : acked.entrySet()) {
                    final OffsetEntry offsetEntry = tpOffset.getValue();
                    offsetEntry.updateAckedState(toCommitOffsets.get(tpOffset.getKey()));
                }
            } else {
                LOG.trace("No offsets to commit. {}", toString());
            }
        } catch (Exception e) {
            LOG.error("Exception occurred while committing to Kafka offsets of acked tuples", e);
        }
    }

    private void updateAckedState(TopicPartition tp, OffsetEntry offsetEntry) {
        if (offsetEntry.isEmpty()) {
            acked.remove(tp);
        }
    }

    @Override
    public String toString() {
        return "{" +
                "acked=" + acked +
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
        private final TopicPartition tp;
        private long committedOffset;               // last offset committed to Kafka
        private long nextCommitOffset;              // last continoue offset to be committed in the next commit operation
        private final Set<MessageId> ackedMsgs = new TreeSet<>(OFFSET_COMPARATOR);     // acked messages sorted by ascending order of offset
        private Set<MessageId> toCommitMsgs = new TreeSet<>(OFFSET_COMPARATOR);        // Messages that contain the offsets to be committed in the next commit operation

        public OffsetEntry(TopicPartition tp) {
            this.tp = tp;
            final OffsetAndMetadata committed = kafkaConsumer.committed(tp);
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
            nextCommitOffset = committedOffset;
            MessageId toCommitMsg = null;

            for (MessageId ackedMsg : ackedMsgs) {  // for K matching messages complexity is K*(Log*N). K <= N
                if ((currOffset = ackedMsg.offset()) != nextCommitOffset) {
                    if (currOffset == nextCommitOffset + 1) {    // found the next offset to commit
                        toCommitMsgs.add(ackedMsg);
                        toCommitMsg = ackedMsg;
                        nextCommitOffset = currOffset;
                    } else if (ackedMsg.offset() > nextCommitOffset + 1) {    // offset found is not continuous to the offsets listed to go in the next commit, so stop search
                        break;
                    } else {
                        LOG.debug("Unexpected offset found [{}]. {}", ackedMsg.offset(), toString());
                        break;
                    }
                }
            }

            if (!toCommitMsgs.isEmpty()) {
                offsetAndMetadata = new OffsetAndMetadata(nextCommitOffset, toCommitMsg.getMetadata(Thread.currentThread()));
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
                KafkaSpout.this.updateAckedState(tp, this);
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
                    ", nextCommitOffset=" + nextCommitOffset +
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
            commitAckedTuples();
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            LOG.debug("Partitions reassignment. [consumer-group={}, consumer={}, topic-partitions={}]",
                    kafkaSpoutConfig.getConsumerGroupId(), kafkaConsumer, partitions);
        }
    }
}
