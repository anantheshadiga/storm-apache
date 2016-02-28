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
import org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.EARLIEST;
import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.LATEST;
import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.UNCOMMITTED_EARLIEST;
import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.UNCOMMITTED_LATEST;

public class KafkaSpout<K,V> extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSpout.class);
    private static final Comparator<MessageId> OFFSET_COMPARATOR = new OffsetComparator();

    // Storm
    protected SpoutOutputCollector collector;

    // Kafka
    private final KafkaSpoutConfig<K, V> kafkaSpoutConfig;
    private KafkaConsumer<K, V> kafkaConsumer;
    private transient boolean consumerAutoCommitMode;
    private transient FirstPollOffsetStrategy firstPollOffsetStrategy;

    // Bookkeeping
    private KafkaSpoutStream kafkaSpoutStream;
    private KafkaTupleBuilder<K,V> tupleBuilder;
    private transient Timer timer;                                    // timer == null for auto commit mode
    private transient Map<TopicPartition, OffsetEntry> acked;         // emitted tuples that were successfully acked. These tuples will be committed by the commitOffsetsTask or on consumer rebalance
    private transient int maxRetries;                                 // Max number of times a tuple is retried
    private transient boolean initialized;          // Flag indicating that the spout is still undergoing initialization process.
                                                    // Initialization is only complete after the first call to  KafkaSpoutConsumerRebalanceListener.onPartitionsAssigned()


    public KafkaSpout(KafkaSpoutConfig<K,V> kafkaSpoutConfig, KafkaSpoutStream kafkaSpoutStream, KafkaTupleBuilder<K,V> tupleBuilder) {
        this.kafkaSpoutConfig = kafkaSpoutConfig;                 // Pass in configuration
        this.kafkaSpoutStream = kafkaSpoutStream;
        this.tupleBuilder = tupleBuilder;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        initialized = false;

        // Spout internals
        this.collector = collector;
        maxRetries = kafkaSpoutConfig.getMaxTupleRetries();

        // Offset management
        firstPollOffsetStrategy = kafkaSpoutConfig.getFirstPollOffsetStrategy();
        consumerAutoCommitMode = kafkaSpoutConfig.isConsumerAutoCommitMode();

        if (!consumerAutoCommitMode) {     // If it is auto commit, no need to commit offsets manually
            timer = new Timer(kafkaSpoutConfig.getOffsetsCommitFreqMs(), 500, TimeUnit.MILLISECONDS);
            acked = new HashMap<>();
        }

        // Kafka consumer
        kafkaConsumer = new KafkaConsumer<>(kafkaSpoutConfig.getKafkaProps(),
                kafkaSpoutConfig.getKeyDeserializer(), kafkaSpoutConfig.getValueDeserializer());
        kafkaConsumer.subscribe(kafkaSpoutConfig.getSubscribedTopics(), new KafkaSpoutConsumerRebalanceListener());
        // Initial poll to get the consumer registration process going.
        // KafkaSpoutConsumerRebalanceListener will be called foloowing this poll upon partition registration
        kafkaConsumer.poll(0);

        LOG.debug("Kafka Spout opened with the following configuration: {}", kafkaSpoutConfig.toString());
    }

    // ======== Next Tuple =======

    @Override
    public void nextTuple() {
        if (initialized) {
            if(commit()) {
                commitOffsetsForAckedTuples();
            } else {
                emitTuples(poll());
            }
        } else {
            LOG.debug("Spout not initialized. Not sending tuples until initialization completes");
        }
    }

    private boolean commit() {
        return !consumerAutoCommitMode && timer.expired();    // timer != null for non auto commit mode
    }

    private ConsumerRecords<K, V> poll() {
        final ConsumerRecords<K, V> consumerRecords = kafkaConsumer.poll(kafkaSpoutConfig.getPollTimeoutMs());
        LOG.debug("Polled [{}] records from Kafka", consumerRecords.count());
        return consumerRecords;
    }

    private void emitTuples(ConsumerRecords<K, V> consumerRecords) {
        for (TopicPartition tp : consumerRecords.partitions()) {
            final Iterable<ConsumerRecord<K, V>> records = consumerRecords.records(tp.topic());     // TODO Decide if want to give flexibility to emmit/poll either per topic or per partition

            for (final ConsumerRecord<K, V> record : records) {
                if (record.offset() == 0 || record.offset() > acked.get(tp).committedOffset) {      // The first poll includes the last committed offset. This if avoids duplication
                    final List<Object> tuple = tupleBuilder.buildTuple(record);
                    final MessageId messageId = new MessageId(record, tuple);                                  // TODO don't create message for non acking mode. Should we support non acking mode?

                    collector.emit(kafkaSpoutStream.getStreamId(), tuple, messageId);           // emits one tuple per record
                    LOG.debug("Emitted tuple [{}] for record [{}]", tuple, record);
                }
            }
        }
    }

    // ======== Ack =======
    @Override
    public void ack(Object messageId) {
        final MessageId msgId = (MessageId) messageId;
        final TopicPartition tp = msgId.getTopicPartition();

        if (!consumerAutoCommitMode) {  // Only need to keep track of acked tuples if commits are not done automatically
            acked.get(tp).add(msgId);
            LOG.debug("Adding acked message to [{}] to list of messages to be committed to Kafka", msgId);
        }
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
        if(!consumerAutoCommitMode) {
            commitOffsetsForAckedTuples();
        }
    }

    @Override
    public void close() {
        try {
            kafkaConsumer.wakeup();
            if(!consumerAutoCommitMode) {
                commitOffsetsForAckedTuples();
            }
        } finally {
            //remove resources
            kafkaConsumer.close();
        }
    }

    // TODO must declare multiple output streams
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(kafkaSpoutStream.getStreamId(), kafkaSpoutStream.getOutputFields());
    }

    // ====== Private helper methods ======

    private void commitOffsetsForAckedTuples() {
        final Map<TopicPartition, OffsetAndMetadata> nextCommitOffsets = new HashMap<>();

        try {
            for (Map.Entry<TopicPartition, OffsetEntry> tpOffset : acked.entrySet()) {
                final OffsetAndMetadata offsetAndMetadata = tpOffset.getValue().findNextCommitOffset();
                if (offsetAndMetadata != null) {
                    nextCommitOffsets.put(tpOffset.getKey(), offsetAndMetadata);
                }
            }

            if (!nextCommitOffsets.isEmpty()) {
                kafkaConsumer.commitSync(nextCommitOffsets);
                LOG.debug("Offsets successfully committed to Kafka [{}]", nextCommitOffsets);
                // Instead of iterating again, we could commit and update state for each TopicPartition in the prior loop,
                // but the multiple network calls should be more expensive than iterating twice over a small loop
                for (Map.Entry<TopicPartition, OffsetEntry> tpOffset : acked.entrySet()) {
                    final OffsetEntry offsetEntry = tpOffset.getValue();
                    offsetEntry.updateAckedState(nextCommitOffsets.get(tpOffset.getKey()));
                }
            } else {
                LOG.trace("No offsets to commit. {}", toString());
            }
        } catch (Exception e) {
            LOG.error("Exception occurred while committing to Kafka offsets of acked tuples", e);
        }
    }

    @Override
    public String toString() {
        return "{acked=" + acked + "} ";
    }

    // ======= Offsets Commit Management ==========

    private static class OffsetComparator implements Comparator<MessageId> {
        public int compare(MessageId m1, MessageId m2) {
            return m1.offset() < m2.offset() ? -1 : m1.offset() == m2.offset() ? 0 : 1;
        }
    }

    /** This class is not thread safe */
    private class OffsetEntry {
        private final TopicPartition tp;
        private long committedOffset;               // last offset committed to Kafka, or initial fetching offset (initial value depends on offset strategy. See KafkaSpoutConsumerRebalanceListener)
        private final NavigableSet<MessageId> ackedMsgs = new TreeSet<>(OFFSET_COMPARATOR);     // acked messages sorted by ascending order of offset

        public OffsetEntry(TopicPartition tp, long committedOffset) {
            this.tp = tp;
            this.committedOffset = committedOffset;
            LOG.debug("Created OffsetEntry for [topic-partition={}, committed-or-initial-fetch-offset={}]", tp, committedOffset);
        }

        public void add(MessageId msgId) {          // O(Log N)
            ackedMsgs.add(msgId);
        }

        /**
         * This method has side effects. The method updateAckedState should be called after this method.
         * @return the next OffsetAndMetadata to commit, or null if no offset is ready to commit.
         */
        public OffsetAndMetadata findNextCommitOffset() {
            boolean found = false;
            long currOffset;
            long nextCommitOffset = committedOffset;
            MessageId nextCommitMsg = null;     // this is a convenience field to make it faster to create OffsetAndMetadata
                                                    //TODO Offset by one issue or don't remove from acks issue
            for (MessageId currAckedMsg : ackedMsgs) {  // complexity is that of a linear scan on a TreeMap
                if ((currOffset = currAckedMsg.offset()) == 0 || currOffset != nextCommitOffset) {     // this is to void duplication because the first message polled is the last message acked.
                    if (currOffset == nextCommitOffset || currOffset == nextCommitOffset + 1) {            // found the next offset to commit
                        found = true;
                        nextCommitMsg = currAckedMsg;
                        nextCommitOffset = currOffset;
                        LOG.trace("Found offset to commit [{}]. {}", currOffset, toString());
                    } else if (currAckedMsg.offset() > nextCommitOffset + 1) {    // offset found is not continuous to the offsets listed to go in the next commit, so stop search
                        LOG.debug("Non continuous offset found [{}]. It will be processed in a subsequent batch. {}", currOffset, toString());
                        break;
                    } else {
                        LOG.debug("Unexpected offset found [{}]. {}", currOffset, toString());
                        break;
                    }
                }
            }

            OffsetAndMetadata offsetAndMetadata = null;
            if (found) {
                offsetAndMetadata = new OffsetAndMetadata(nextCommitOffset, nextCommitMsg.getMetadata(Thread.currentThread()));
                LOG.trace("Offset to be committed next: [{}] {}", offsetAndMetadata.offset(), toString());
            } else {
                LOG.debug("No offsets ready to commit", toString());
            }
            return offsetAndMetadata;
        }

        /**
         * This method has side effects and should be called after findNextCommitOffset
         */
        public void updateAckedState(OffsetAndMetadata offsetAndMetadata) {
            if (offsetAndMetadata != null) {
                committedOffset = offsetAndMetadata.offset();
                for (Iterator<MessageId> iterator1 = ackedMsgs.iterator();
                     iterator1.hasNext(); ) {
                    if (iterator1.next().offset() <= offsetAndMetadata.offset()) {
                        iterator1.remove();
                    } else {
                        break;
                    }
                }
            }
            LOG.trace("Object state after update: {}", toString());
        }

        public boolean isEmpty() {
            return ackedMsgs.isEmpty();
        }

        @Override
        public String toString() {
            return "OffsetEntry{" +
                    "topic-partition=" + tp +
                    ", committedOffset=" + committedOffset +
                    ", ackedMsgs=" + ackedMsgs +
                    '}';
        }
    }

    // =========== Consumer Rebalance Listener - On the same thread as the caller ===========

    private class KafkaSpoutConsumerRebalanceListener implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            LOG.debug("Partitions revoked. [consumer-group={}, consumer={}, topic-partitions={}]",
                    kafkaSpoutConfig.getConsumerGroupId(), kafkaConsumer, partitions);
            if (!consumerAutoCommitMode && initialized) {
                initialized = false;
                commitOffsetsForAckedTuples();
            }
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            LOG.debug("Partitions reassignment. [consumer-group={}, consumer={}, topic-partitions={}]",
                    kafkaSpoutConfig.getConsumerGroupId(), kafkaConsumer, partitions);

            initialize(partitions);
        }

        private void initialize(Collection<TopicPartition> partitions) {
            acked.keySet().retainAll(partitions);   // remove from acked all partitions that are no longer assigned to this spout
            for (TopicPartition tp: partitions) {
                final OffsetAndMetadata committedOffset = kafkaConsumer.committed(tp);
                final long fetchOffset = doSeek(tp, committedOffset);
                setAcked(tp, fetchOffset);
            }
            initialized = true;
            LOG.debug("Initialization complete");
        }

        /** sets the cursor to the location dictated by the first poll strategy and returns the fetch offset */
        private long doSeek(TopicPartition tp, OffsetAndMetadata committedOffset) {
            long fetchOffset;
            if (committedOffset != null)  {     // fetchOffset was committed for this TopicPartition
                if (firstPollOffsetStrategy.equals(EARLIEST)) {
                    kafkaConsumer.seekToBeginning(tp);
                    fetchOffset = kafkaConsumer.position(tp);
                } else if (firstPollOffsetStrategy.equals(LATEST)) {
                    kafkaConsumer.seekToEnd(tp);
                    fetchOffset = kafkaConsumer.position(tp);
                } else {
                    // do nothing - by default polling starts at the lat committed fetchOffset
                    fetchOffset = committedOffset.offset();
                }
            } else {    // no previous commit occurred, so start at the beginning or end depending on the strategy
                if (firstPollOffsetStrategy.equals(EARLIEST) || firstPollOffsetStrategy.equals(UNCOMMITTED_EARLIEST)) {
                    kafkaConsumer.seekToBeginning(tp);
                } else if (firstPollOffsetStrategy.equals(LATEST) || firstPollOffsetStrategy.equals(UNCOMMITTED_LATEST)) {
                    kafkaConsumer.seekToEnd(tp);
                }
                fetchOffset = kafkaConsumer.position(tp);
            }
            return fetchOffset;
        }

        private void setAcked(TopicPartition tp, long fetchOffset) {
            if (!consumerAutoCommitMode && !acked.containsKey(tp)) {           // If this partition was previously assigned, leave the acked offsets as they were to resume where it left off
                acked.put(tp, new OffsetEntry(tp, fetchOffset));
            }
        }
    }

    // =========== Timer ===========

    private class Timer {
        private final long frequency;
        private final long delay;
        private final TimeUnit timeUnit;
        private final long frequencyNanos;
        private long start;

        /** Creates a timer to expire at the given frequency and starting with the specified time delay.
         *  Frequency and delay must be specified in the same TimeUnit */
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

        /**
         * If this method returns true, a new timer cycle will start.
         * @return true if the elapsed time since the last true value call to this method is greater or
         * equal to the frequency specified upon creation of this timer. Returns false otherwise.
         */
        public boolean expired() {
            final boolean expired = System.nanoTime() - start > frequencyNanos;
            if (expired) {
                start = System.nanoTime();
            }
            return expired;
        }
    }
}
