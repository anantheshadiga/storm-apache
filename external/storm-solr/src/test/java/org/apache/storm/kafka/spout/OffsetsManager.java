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
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class OffsetsManager<K,V> implements IOffsetsManager {
    protected static final Logger log = LoggerFactory.getLogger(OffsetsManager.class);

    private KafkaSpout kafkaSpout;
    private KafkaConsumer<K, V> kafkaConsumer;
    Map<TopicPartition, OffsetEntry> acked = new HashMap<>();
    final Map<TopicPartition, Set<MessageId>> failed = new HashMap<>();

    public OffsetsManager(KafkaSpout kafkaSpout, KafkaConsumer<K,V> kafkaConsumer) {
        this.kafkaSpout = kafkaSpout;
        this.kafkaConsumer = kafkaConsumer;
    }

    @Override
    public void ack(MessageId msgId) {
        final TopicPartition tp = msgId.getTopicPartition();

        kafkaSpout.ackedLock.lock();
        try {
            if (!acked.containsKey(tp)) {
                acked.put(tp, new OffsetEntry(null, null));
            }

            final OffsetEntry offsetEntry = acked.get(tp);
            offsetEntry.insert(msgId);
        } finally {
            kafkaSpout.ackedLock.unlock();
        }

        // Removed acked tuples from the emittedTuples data structure
        kafkaSpout.emittedTuples.remove(msgId);

        // if acked message is a retry, remove it from failed data structure
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
            log.debug("Reached the maximum number of retries. Adding message {[]} to list of messages to be committed to kafka", msgId);
            ack(msgId);
            msgIds.remove(msgId);
            if (msgIds.isEmpty()) {
                failed.remove(tp);
            }
        }
    }


    @Override
    public boolean retry() {
        return failed.size() > 0;
    }

    @Override
    public void retryFailed() {
        for (TopicPartition tp: failed.keySet()) {
            for (MessageId msgId : failed.get(tp)) {
                Values tuple = kafkaSpout.emittedTuples.get(msgId);
                log.debug("Retrying tuple. [msgId={}, tuple={}]", msgId, tuple);
                kafkaSpout.collector.emit(kafkaSpout.getStreamId(), tuple, msgId);
            }
        }
    }

    /** Commits to kafka the maximum sequence of continuous offsets that have been acked for a partition */
    @Override
    public void commitAckedOffsets() {
        final Map<TopicPartition, OffsetAndMetadata> ackedOffsets = new HashMap<>();
        for (TopicPartition tp : acked.keySet()) {
            final MessageId msgId = acked.get(tp).getMaxOffsetMsgAcked();
            ackedOffsets.put(tp, new OffsetAndMetadata(msgId.offset(), msgId.getMetadata(Thread.currentThread())));
        }

        kafkaSpout.kafkaConsumer.commitSync(ackedOffsets);
        log.debug("Offsets successfully committed to Kafka {[]}", ackedOffsets);

        // All acked offsets have been committed, so clean data structure
        acked = new HashMap<>();
    }

    // TODO
    private int maxRetries() {
        return Integer.MAX_VALUE;
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

            if (offsets.isEmpty() || msgId.offset() == (getLastOffset() + 1)) {           // msgId becomes last element of this offsets sublist
                setLast(msgId);
            } else if (msgId.offset() == (getFirstOffset() - 1)) {   // msgId becomes first element of this offsets sublist
                setFirst(msgId);
            } else if (msgId.offset() < getFirstOffset()) {           // insert a new OffsetEntry element in the list
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
            if (isHead() && !offsets.isEmpty() && offsets.get(0).offset() == lastCommittedOffset + 1) {
                msgId = offsets.get(offsets.size() - 1);
                lastCommittedOffset = msgId.offset();
            }
            return msgId;
        }

        private long getLastOffset() {
            return offsets.isEmpty() ? -1 : offsets.get(offsets.size() - 1).offset();
        }

        private long getFirstOffset() {
            return offsets.isEmpty() ? -1 : offsets.get(0).offset();
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
    
}
