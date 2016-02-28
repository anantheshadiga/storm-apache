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
//    private transient boolean firstPollComplete;  // TODO Delete
    private transient boolean open;                 // Flat that is set to true once the open method completes. This is used by the callback KafkaSpoutConsumerRebalanceListener
                                                    // to guarantee that open method has completed and all the state is set before the callback
    private transient boolean initialized;          // Flag indicating that the spout is still undergoing initialization process.
                                                    // Initialization is only complete after the first call to  KafkaSpoutConsumerRebalanceListener.onPartitionsAssigned()


    public KafkaSpout(KafkaSpoutConfig<K,V> kafkaSpoutConfig, KafkaSpoutStream kafkaSpoutStream, KafkaTupleBuilder<K,V> tupleBuilder) {
        this.kafkaSpoutConfig = kafkaSpoutConfig;                 // Pass in configuration
        this.kafkaSpoutStream = kafkaSpoutStream;
        this.tupleBuilder = tupleBuilder;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        open = false;
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

        open = true;
        LOG.debug("Kafka Spout opened with the following configuration: {}", kafkaSpoutConfig.toString());
    }

    // ======== Next Tuple =======

    @Override
    public void nextTuple() {
        if (initialized) {
//        if (true) {
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

            int i = 0;
            for (Iterator<ConsumerRecord<K, V>> iterator = records.iterator(); iterator.hasNext(); ) {
                final ConsumerRecord<K, V> record = iterator.next();
                final List<Object> tuple = tupleBuilder.buildTuple(record);
                final MessageId messageId = new MessageId(record, tuple);                                  // TODO don't create message for non acking mode. Should we support non acking mode?

                collector.emit(kafkaSpoutStream.getStreamId(), tuple, messageId);           // emits one tuple per record
                LOG.debug("Emitted tuple [{}] for record [{}]", tuple, record);

                // We need to create OffsetEntry here because there is no way to retrieve the largest non committed offset from KafkaConsumer
                // creates OffsetEntry with the latest, non-committed offset, if the strategy is LATEST
                if (!consumerAutoCommitMode && !iterator.hasNext() /*it's last record */ && !acked.containsKey(tp)) {
                    acked.put(tp, new OffsetEntry(tp, committedOffset(tp, record)));
                }
            }
        }
    }

    /**
     * @return 0 if strategy is EARLIEST, largest, non-committed offset, if the strategy is LATEST, or largest committed offset otherwise
     */
    private long committedOffset(TopicPartition tp, ConsumerRecord<K, V> record) {
        switch(firstPollOffsetStrategy) {
            case EARLIEST:
                return 0;       //TODO Bug in here
            case LATEST:
                return record.offset();
            case UNCOMMITTED_EARLIEST:
            case UNCOMMITTED_LATEST:
                return kafkaConsumer.committed(tp) .offset();
            default:
                break;
        }
        return record.offset();
    }

    // ======== Ack =======
    @Override
    public void ack(Object messageId) {
        final MessageId msgId = (MessageId) messageId;
        final TopicPartition tp = msgId.getTopicPartition();

        //TODO: NPE

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

    private void updateAckedState(TopicPartition tp, OffsetEntry offsetEntry) {
        if (offsetEntry.isEmpty()) {
            acked.remove(tp);
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
        private long committedOffset;               // last offset committed to Kafka
        private long nextCommitOffset;              // last continoue offset to be committed in the next commit operation
        private final NavigableSet<MessageId> ackedMsgs = new TreeSet<>(OFFSET_COMPARATOR);     // acked messages sorted by ascending order of offset
        private NavigableSet<MessageId> nextCommitMsgs = new TreeSet<>(OFFSET_COMPARATOR);        // Messages that contain the offsets to be committed in the next commit operation

        public OffsetEntry(TopicPartition tp, long committedOffset) {
            this.tp = tp;
            this.committedOffset = committedOffset;
            LOG.debug("Created OffsetEntry for [topic-partition={}, last-committed-offset={}]", tp, committedOffset);
        }

        public void add(MessageId msgId) {          // O(Log N)
            ackedMsgs.add(msgId);
        }

        /**
         * This method has side effects. The method updateAckedState should be called after this method.
         * @return the next OffsetAndMetadata to commit, or null if no offset is ready to commit.
         */
        public OffsetAndMetadata findNextCommitOffset() {
            long currOffset;
            MessageId nextCommitMsg = null;     // this convenience field is used to make it faster to create OffsetAndMetadata
            nextCommitMsgs = new TreeSet<>(OFFSET_COMPARATOR);
            nextCommitOffset = committedOffset;
                                                    //TODO Offset by one issue or don't remove from acks issue
            for (MessageId ackedMsg : ackedMsgs) {  // for K matching messages complexity is K*(Log*N). K <= N
                if ((currOffset = ackedMsg.offset()) != nextCommitOffset) {     // this is to void duplication because the first message polled is the last message acked.
                    if (currOffset == nextCommitOffset + 1) {                   // found the next offset to commit
                        nextCommitMsgs.add(ackedMsg);
                        nextCommitMsg = ackedMsg;
                        nextCommitOffset = currOffset;
                        LOG.trace("Found offset to commit [{}]. {}", currOffset, toString());
                    } else if (ackedMsg.offset() > nextCommitOffset + 1) {    // offset found is not continuous to the offsets listed to go in the next commit, so stop search
                        LOG.debug("Non continuous offset found [{}]. It will be processed in a subsequent batch. {}", currOffset, toString());
                        break;
                    } else {
                        LOG.debug("Unexpected offset found [{}]. {}", currOffset, toString());
                        break;
                    }
                }
            }

            OffsetAndMetadata offsetAndMetadata = null;
            if (!nextCommitMsgs.isEmpty()) {
                offsetAndMetadata = new OffsetAndMetadata(nextCommitOffset, nextCommitMsg.getMetadata(Thread.currentThread()));
                LOG.debug("Offset to be committed next: [{}]", offsetAndMetadata.offset());
                LOG.trace(toString());
            } else {
                LOG.debug("No offsets ready to commit");
                LOG.trace(toString());
            }
            return offsetAndMetadata;
        }

        /**
         * This method has side effects and should be called after findNextCommitOffset
         */
        public void updateAckedState(OffsetAndMetadata offsetAndMetadata) {
            if (offsetAndMetadata != null) {
                committedOffset = offsetAndMetadata.offset();
                ackedMsgs.removeAll(nextCommitMsgs);
                nextCommitMsgs = new TreeSet<>(OFFSET_COMPARATOR);
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
                    ", nextCommitMsgs=" + nextCommitMsgs +
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
                commitOffsetsForAckedTuples();
            }
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            /*if (!open) {
                throw new IllegalStateException("Spout's open() method should have completed before partitions get assigned");
            }*/

            LOG.debug("Partitions reassignment. [consumer-group={}, consumer={}, topic-partitions={}]",
                    kafkaSpoutConfig.getConsumerGroupId(), kafkaConsumer, partitions);

            if (!initialized) {         // This logic should be run only once, during the first time during spout initialization
                initialize(partitions);
            }
        }

        private void initialize(Collection<TopicPartition> partitions) {
            // Save the last committed offsets. This is done before the poll(0) bellow to assure that the dummy, but necessary,
            // poll(0) does not cause the meaningful polls to start at the wrong offset
            final Map<TopicPartition, OffsetAndMetadata> committedOffsets = new HashMap<>();
            for (TopicPartition tp: partitions) {
                committedOffsets.put(tp, kafkaConsumer.committed(tp));
                LOG.debug("Kafka consumer state snapshot at initialization: {}", committedOffsets);
            }

            kafkaConsumer.poll(0);  // this is a temporary workaround to avoid a Kafka limitation: Kafka throws IllegalStateException if seek(..) is called before commit(..)

            for (TopicPartition tp: partitions) {
                doSeek(tp, committedOffsets.get(tp));
            }
            initialized = true;
            LOG.debug("Initialization complete");
        }

        private void doSeek(TopicPartition tp, OffsetAndMetadata committedOffset) {
            if (committedOffset != null)  {     // offset were committed for this TopicPartition
                if (firstPollOffsetStrategy.equals(EARLIEST)) {
                    kafkaConsumer.seekToBeginning(tp);
                } else if (firstPollOffsetStrategy.equals(LATEST)) {
                    kafkaConsumer.seekToEnd(tp);
                } else {
                    // do nothing - by default polling starts at the lat committed offset
//                    kafkaConsumer.seek(tp, committedOffset.offset());
                }
            } else {    // no previous commit occurred, so start at the beginning or end depending on the strategy
                if (firstPollOffsetStrategy.equals(EARLIEST) || firstPollOffsetStrategy.equals(UNCOMMITTED_EARLIEST)) {
                    kafkaConsumer.seekToBeginning(tp);
                } else if (firstPollOffsetStrategy.equals(LATEST) || firstPollOffsetStrategy.equals(UNCOMMITTED_LATEST)) {
                    kafkaConsumer.seekToEnd(tp);
                }
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
