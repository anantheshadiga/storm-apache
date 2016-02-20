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

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class SimpleOffsetsManager implements IOffsetsManager {
    private static final Logger log = LoggerFactory.getLogger(SimpleOffsetsManager.class);
    private final Comparator<MessageId> OFFSET_COMPARATOR = new OffsetComparator();

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

    private static class OffsetComparator implements Comparator<MessageId> {
        public int compare(MessageId m1, MessageId m2) {
            return m1.offset() < m2.offset() ? -1 : m1.offset() == m2.offset() ? 0 : 1;
        }
    }

    private class OffsetEntry {
        private long committedOffset = -1;          // last offset committed to Kafka
        private long toCommitOffset;                // last offset to be committed in the next commit operation
        private final Set<MessageId> ackedMsgs = new TreeSet<>(OFFSET_COMPARATOR);    // sort messages by ascending order of offset
        private Set<MessageId> toCommitMsgs;        // Messages that contain the offsets to be committed in thee next commit operation

        public void add(MessageId msgId) {          // O(Log N)
            ackedMsgs.add(msgId);
        }

        /**
         * This method has side effects. updateState method should be call after this method.
         */
        public OffsetAndMetadata findOffsetToCommit() {
            long currOffset;
            OffsetAndMetadata offsetAndMetadata = null;
            toCommitMsgs = new TreeSet<>(OFFSET_COMPARATOR);
            toCommitOffset = committedOffset;
            MessageId toCommitMsg = null;

            int i = 0;
            for (MessageId ackedMsg : ackedMsgs) {  // for K matching messages complexity is K*(Log*N). K <= N
                if (i == 0 && ackedMsg.offset() > committedOffset + 1) { // the first acked offset found is not the next offset to be committed,
                    break;                                               // so the next offset to be committed has not been acked yet and nothing can be committed
                } else if ((currOffset = ackedMsg.offset()) == toCommitOffset + 1) {    // found the next offset to commit
                    toCommitMsgs.add(ackedMsg);
                    toCommitOffset = currOffset;
                    toCommitMsg = ackedMsg;
                    i++;
                } else if (ackedMsg.offset() > toCommitOffset + 1) {    // offset found is not continuous to the offsets listed to go in the next commit, so stop search
                    break;
                } else {
                    log.debug("Unexpected offset found {[]}. Last committed offset {[]}",
                            ackedMsg.offset(), committedOffset);
                    break;
                }
            }

            if (!toCommitMsgs.isEmpty()) {
                offsetAndMetadata = new OffsetAndMetadata(toCommitOffset, toCommitMsg.getMetadata(Thread.currentThread()));
                log.debug("Last offset to be committed in the next commit call: {[]}", offsetAndMetadata.offset());
                log.trace(toString());
            } else {
                log.debug("No offsets found ready to commit" );
                log.trace(toString());
            }
            // TODO
            // no messages;
            // found all the way to the last message

            return offsetAndMetadata;
        }

        /**
         * This method has side effects and should be called after findOffsetToCommit
         */
        public void updateState(OffsetAndMetadata offsetAndMetadata) {
            //TODO offsets
            committedOffset = offsetAndMetadata.offset();
            ackedMsgs.removeAll(toCommitMsgs);
            toCommitMsgs = null;
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
        for (TopicPartition tp : failed.keySet()) {
            for (MessageId msgId : failed.get(tp)) {
                final Values tuple = emittedTuples.get(msgId);
                log.debug("Retrying tuple. [msgId={}, tuple={}]", msgId, tuple);
                collector.emit(kafkaStream.getStreamId(), tuple, msgId);
            }
        }
    }

    @Override
    public void commitAckedOffsets() {
        final Map<TopicPartition, OffsetAndMetadata> toCommitOffsets = new HashMap<>();
        for (TopicPartition tp : acked.keySet()) {
            OffsetEntry offsetEntry = acked.get(tp);
            OffsetAndMetadata offsetAndMetadata = offsetEntry.findOffsetToCommit();
            if (offsetAndMetadata != null) {
                toCommitOffsets.put(tp, offsetAndMetadata);
            }
        }

        kafkaConsumer.commitSync(toCommitOffsets);
        log.debug("Offsets successfully committed to Kafka {[]}", toCommitOffsets);


        // Instead of iterating again, the other option would be to commit each TopicPartition and update in the same iteration,
        // but the multiple networks calls should be more expensive than iteration twice over a small loop
        for (TopicPartition tp : acked.keySet()) {
            OffsetEntry offsetEntry = acked.get(tp);
            offsetEntry.updateState(toCommitOffsets.get(tp));
        }

        // All acked offsets have been committed, so clean data structure
        acked = new HashMap<>();

    }

    @Override
    public void putEmitted(MessageId messageId, Values tuple) {
        emittedTuples.put(messageId, tuple);
    }
}
