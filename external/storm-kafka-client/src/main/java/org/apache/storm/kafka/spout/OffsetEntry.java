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
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.TreeSet;

public class OffsetEntry {
    private static final Logger LOG = LoggerFactory.getLogger(OffsetEntry.class);
    public static final Comparator<KafkaSpoutMessageId> OFFSET_COMPARATOR = new OffsetComparator();


    private final TopicPartition tp;
    private final long initialFetchOffset;  /* First offset to be fetched. It is either set to the beginning, end, or to the first uncommitted offset.
                                                 * Initial value depends on offset strategy. See KafkaSpoutConsumerRebalanceListener */
    private long committedOffset;     // last offset committed to Kafka. Initially it is set to fetchOffset - 1
//    private final NavigableSet<KafkaSpoutMessageId> ackedMsgs = new TreeSet<>(OFFSET_COMPARATOR);     // acked messages sorted by ascending order of offset
    private NavigableSet<KafkaSpoutMessageId> ackedMsgs ;

    private static class OffsetComparator implements Comparator<KafkaSpoutMessageId> {
        public int compare(KafkaSpoutMessageId m1, KafkaSpoutMessageId m2) {
            return m1.offset() < m2.offset() ? -1 : m1.offset() == m2.offset() ? 0 : 1;
        }
    }

    public OffsetEntry(TopicPartition tp, long initialFetchOffset) {
        this.tp = tp;
        this.initialFetchOffset = initialFetchOffset;
        this.committedOffset = initialFetchOffset - 1;
        ackedMsgs = new TreeSet<>(OFFSET_COMPARATOR);
        LOG.debug("Instantiated {}", this);
    }

    public OffsetEntry(TopicPartition tp, long initialFetchOffset, NavigableSet<KafkaSpoutMessageId> ackedMsgs) {
        this.tp = tp;
        this.initialFetchOffset = initialFetchOffset;
        this.committedOffset = initialFetchOffset - 1;
        this.ackedMsgs = ackedMsgs;
        LOG.debug("Instantiated {}", this);
    }

    public void add(KafkaSpoutMessageId msgId) {          // O(Log N)
        ackedMsgs.add(msgId);
    }

    /**
     * @return the next OffsetAndMetadata to commit, or null if no offset is ready to commit.
     */
    public OffsetAndMetadata findNextCommitOffset() {
        boolean found = false;
        long currOffset;
        long nextCommitOffset = committedOffset;
        KafkaSpoutMessageId nextCommitMsg = null;     // this is a convenience variable to make it faster to create OffsetAndMetadata

        int i = 0;
        for (KafkaSpoutMessageId currAckedMsg : ackedMsgs) {  // complexity is that of a linear scan on a TreeMap
            if ((currOffset = currAckedMsg.offset()) == initialFetchOffset || currOffset == nextCommitOffset + 1) {            // found the next offset to commit
                found = true;
                nextCommitMsg = currAckedMsg;
                nextCommitOffset = currOffset;
                LOG.trace("Found offset to commit [{}]. {}", currOffset, this);
            } else if (currAckedMsg.offset() > nextCommitOffset + 1) {    // offset found is not continuous to the offsets listed to go in the next commit, so stop search
                LOG.debug("Non continuous offset found [{}]. It will be processed in a subsequent batch. {}", currOffset, this);
                break;
            } else {
                LOG.debug("Unexpected offset found [{}]. {}", currOffset, this);
                break;
            }
        }

        OffsetAndMetadata nextCommitOffsetAndMetadata = null;
        if (found) {
            nextCommitOffsetAndMetadata = new OffsetAndMetadata(nextCommitOffset, nextCommitMsg.getMetadata(Thread.currentThread()));
            LOG.debug("Offset to be committed next: [{}] {}", nextCommitOffsetAndMetadata.offset(), this);
        } else {
            LOG.debug("No offsets ready to commit. {}", this);
        }
        return nextCommitOffsetAndMetadata;
    }

    /**
     * Marks an offset has committed. This method has side effects - it sets the internal state in such a way that future
     * calls to {@link #findNextCommitOffset()} will return offsets greater than the offset specified, if any.
     *
     * @param committedOffset offset to be marked as committed
     */
    public void commit(OffsetAndMetadata committedOffset) {
        if (committedOffset != null) {
            final long numCommittedOffsets = committedOffset.offset() - this.committedOffset;
            this.committedOffset = committedOffset.offset();
            for (Iterator<KafkaSpoutMessageId> iterator = ackedMsgs.iterator(); iterator.hasNext(); ) {
                if (iterator.next().offset() <= committedOffset.offset()) {
                    iterator.remove();
                } else {
                    break;
                }
            }
//            numUncommittedOffsets-= numCommittedOffsets;
        }
//        LOG.trace("Object state after update: {}, numUncommittedOffsets [{}]", this, numUncommittedOffsets);
        LOG.trace("Object state after update: {}", this);
    }

    public boolean isEmpty() {
        return ackedMsgs.isEmpty();
    }

    public boolean contains(ConsumerRecord record) {
        return contains(new KafkaSpoutMessageId(record));
    }

    public boolean contains(KafkaSpoutMessageId msgId) {
        return ackedMsgs.contains(msgId);
    }

    @Override
    public String toString() {
        return "OffsetEntry{" +
                "topic-partition=" + tp +
                ", fetchOffset=" + initialFetchOffset +
                ", committedOffset=" + committedOffset +
                ", ackedMsgs=" + ackedMsgs +
                '}';
    }
}