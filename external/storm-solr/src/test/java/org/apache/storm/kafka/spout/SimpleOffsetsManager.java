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

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.kafka.spout.strategy.KafkaSpoutStreamDetails;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class SimpleOffsetsManager implements IOffsetsManager {
    private static final Logger log = LoggerFactory.getLogger(SimpleOffsetsManager.class);

    private transient final KafkaConsumer kafkaConsumer;
    // Keeps a list of emitted tuples that are pending ack. Tuples are removed from this list only after being acked
    private transient Map<MessageId, Values> emittedTuples;
    private transient final Map<TopicPartition, Set<MessageId>> failed;     // failed tuples. They stay in this list until success or max retries is reached
    private transient final Map<TopicPartition, OffsetEntry> acked;         // emitted tuples that were successfully acked. These tuples will be committed by the commitOffsetsTask
    private final SpoutOutputCollector collector;
    private final KafkaSpoutStreamDetails kafkaStream;


    public SimpleOffsetsManager(SpoutOutputCollector collector, KafkaSpoutStreamDetails kafkaStream, KafkaConsumer kafkaConsumer) {
        this.collector = collector;
        this.kafkaStream = kafkaStream;
        this.kafkaConsumer = kafkaConsumer;
        emittedTuples = new HashMap<>();
        failed = new HashMap<>();
        acked = new HashMap<>();
    }


    private class OffsetEntry {
        private long largestCommittedOffset = 0;
        private Set<MessageId> ackedMsgs = new TreeSet<>();

        public void add(MessageId msgId) {
            ackedMsgs.add(msgId);
        }

        public long getLargestCommittedOffset() {
            return largestCommittedOffset;
        }

        public void setLargestCommittedOffset(long largestCommittedOffset) {
            this.largestCommittedOffset = largestCommittedOffset;
        }

        public void getLargestOffsetReadyToCommit() {

        }

        public OffsetAndMetadata commitAckedOffsets() {
            OffsetAndMetadata omdta;

            long prevOffset;

            for (MessageId ackedMsg : ackedMsgs) {

            }

            final Map<TopicPartition, OffsetAndMetadata> ackedOffsets = new HashMap<>();
            for (TopicPartition tp : acked.keySet()) {
                OffsetEntry offsetEntry = acked.get(tp);


                final MessageId msgId = acked.get(tp).getMaxOffsetMsgAcked();
                ackedOffsets.put(tp, new OffsetAndMetadata(msgId.offset(), msgId.metadata(Thread.currentThread())));
            }

            kafkaConsumer.commitSync(ackedOffsets);
            log.debug("Offsets successfully committed to Kafka {[]}", ackedOffsets);

            // All acked offsets have been committed, so clean data structure
            acked = new HashMap<>();
        }
    }

    @Override
    public void ack(MessageId msgId) {
        final TopicPartition tp = msgId.getTopicPartition();

        kafkaSpout.ackedLock.lock();        // TODO Racing conditions with the commitOffsetJob

        if (!acked.containsKey(tp)) {
            acked.put(tp, new OffsetEntry());
        }

        final OffsetEntry offsetEntry = acked.get(tp);
        offsetEntry.add(msgId);

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

    @Override
    public void fail(MessageId msgId) {
        final TopicPartition tp = msgId.getTopicPartition();
        if (!failed.containsKey(tp)) {
            failed.put(tp, new HashSet<MessageId>());
        }

        final Set<MessageId> msgIds = failed.get(tp);
        msgId.incrementNumFails();        // increment number of failures counter
        msgIds.add(msgId);

        // limit to max number of retries
        if (msgId.numFails() >= maxRetries()) {
            log.debug("Reached the maximum number of retries. Adding {[]} to list of messages to be committed to kafka", msgId);
            ack(msgId);
            msgIds.remove(msgId);
            if (msgIds.isEmpty()) {
                failed.remove(tp);
            }
        }
    }

    private int maxRetries() {
        TODO
    }

    @Override
    public boolean retry() {
        return failed.size() > 0;
    }

    @Override
    public void retryFailed() {
        for (TopicPartition tp: failed.keySet()) {
            for (MessageId msgId : failed.get(tp)) {
                final Values tuple = emittedTuples.get(msgId);
                log.debug("Retrying tuple. [msgId={}, tuple={}]", msgId, tuple);
                collector.emit(kafkaStream.getStreamId(), tuple, msgId);
            }
        }
    }

    @Override
    public void commitAckedOffsets() {
        final Map<TopicPartition, OffsetAndMetadata> ackedOffsets = new HashMap<>();
        for (TopicPartition tp : acked.keySet()) {
            OffsetEntry offsetEntry = acked.get(tp);
            offsetEntry.commitAckedOffsets();


            final MessageId msgId = acked.get(tp).getMaxOffsetMsgAcked();
            ackedOffsets.put(tp, new OffsetAndMetadata(msgId.offset(), msgId.metadata(Thread.currentThread())));
        }

        kafkaConsumer.commitSync(ackedOffsets);
        log.debug("Offsets successfully committed to Kafka {[]}", ackedOffsets);

        // All acked offsets have been committed, so clean data structure
        acked = new HashMap<>();

    }

    @Override
    public void putEmitted(MessageId messageId, Values tuple) {
        emittedTuples.put(messageId, tuple);
    }
}
