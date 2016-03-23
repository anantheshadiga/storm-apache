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
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class ExponentialBackoffRetry implements RetryService {
    private static final Logger LOG = LoggerFactory.getLogger(ExponentialBackoffRetry.class);
    private static final RetryEntryTimeStampComparator RETRY_ENTRY_TIME_STAMP_COMPARATOR = new RetryEntryTimeStampComparator();

    private Delay delay;
    private int ratio;
    private int maxRetries;

    private Queue<RetryEntry> entryHeap = new PriorityQueue<>(RETRY_ENTRY_TIME_STAMP_COMPARATOR);
    private Set<KafkaSpoutMessageId> entrySet = new HashSet<>();



    // nextRetry = delay + step^(retryCount - 1)


    /**
     * The time stamp of the next retry is scheduled according to the formula (shifted geometric progression):
     * nextRetry = retryCount == 0 ? System.nanoTime() + delay + ratio^(retryCount) : System.nanoTime() + ratio^(retryCount)
     * Note: retryCount and failCount
     * @param delay initial delay
     * @param ratio ratio of the geometric progression
     * @param maxRetries maximum number a tuple is retried before being set for commit
     */
    public ExponentialBackoffRetry(Delay delay, int ratio, int maxRetries) {
        this.delay = delay;
        this.ratio = ratio;
        this.maxRetries = maxRetries;
    }

    public static class Delay {
        private long delayNanos;
        private long delay;
        private TimeUnit timeUnit;

        public Delay(long delay, TimeUnit timeUnit) {
            this.delay = delay;
            this.timeUnit = timeUnit;
            this.delayNanos = timeUnit.toNanos(delay);
        }

        public long delayNanos() {
            return delayNanos;
        }

        public long delay() {
            return delay;
        }

        public TimeUnit timeUnit() {
            return timeUnit;
        }
    }

    @Override
    public KafkaSpoutMessageId next() {
        final RetryEntry entry = entryHeap.peek();
        LOG.debug("Next message to retry [{}]", entry);
        return entry != null && entry.retry() ? entry.msgId : null;
    }

    @Override()
    public boolean update(KafkaSpoutMessageId msgId) {
        boolean updated;
        if(entrySet.contains(msgId)) {
            for (RetryEntry retryEntry : entryHeap) {
                if (retryEntry.msgId.equals(msgId)) {
                    retryEntry.setNextRetryTime();
                    updated = true;
                }
            }
        }

    }

    public <K,V> boolean update(ConsumerRecords<K,V> consumerRecords) {
        Map<TopicPartition, Long> tpToLargestFetchedOffset= new HashMap<>();
        for (TopicPartition tp : consumerRecords.partitions()) {
            final List<ConsumerRecord<K, V>> records = consumerRecords.records(tp);
            tpToLargestFetchedOffset.put(tp, records.get(records.size()-1).offset());
        }




        for (ConsumerRecord<K, V> record : consumerRecords) {
            if (entrySet.contains(new KafkaSpoutMessageId(record)) && )
        }
        boolean updated;
        if(entrySet.contains(msgId)) {
            for (RetryEntry retryEntry : entryHeap) {
                if (retryEntry.msgId.equals(msgId)) {
                    retryEntry.setNextRetryTime();
                    updated = true;
                }
            }
        }

    }

    @Override
    public void remove(KafkaSpoutMessageId msgId) {
    }

    /**
     * Adds if it is the first time. Update if
     * @param msgId
     */
    @Override
    public void add(KafkaSpoutMessageId msgId) {
        if(entrySet.contains(msgId)) {
            for (Iterator<RetryEntry> iterator = entryHeap.iterator(); iterator.hasNext(); ) {
                RetryEntry retryEntry = iterator.next();
                if (retryEntry.msgId.equals(msgId)) {
                    iterator.remove();
                }

            }
            entryHeap.remove(msgId)
        }


    }

    public static class RetryEntries {
        private RetryEntryTimeStampComparator RETRY_ENTRY_TIME_STAMP_COMPARATOR = new RetryEntryTimeStampComparator();
        private Queue<RetryEntry> entryHeap = new PriorityQueue<>(RETRY_ENTRY_TIME_STAMP_COMPARATOR);
        private Set<RetryEntry> entrySet = new HashSet<>();

        public void add(RetryEntry retryEntry) {
            entryHeap.add(retryEntry);
            entrySet.add(retryEntry);
        }

        public boolean remove(RetryEntry retryEntry) {
            entryHeap.remove(retryEntry);
            return entrySet.remove(retryEntry);
        }

        public boolean contains(RetryEntry retryEntry) {
            return entrySet.contains(retryEntry);
        }

        public RetryEntry first() {
            return entryHeap.peek();
        }
    }

    // if value is greater than Long.MAX_VALUE it truncates to Long.MAX_VALUE
    private long nextTime(KafkaSpoutMessageId msgId) {
        return msgId.numFails() == 0
                ? (long) (System.nanoTime() + delay.delayNanos() + Math.pow(ratio, msgId.numFails()))
                : (long) (System.nanoTime() + Math.pow(ratio, msgId.numFails()));
    }


    private class RetryEntry {
        private KafkaSpoutMessageId msgId;
        private long nextRetryTimeNanos;

        public RetryEntry(KafkaSpoutMessageId msgId, long nextRetryTime) {
            this.msgId = msgId;
            this.nextRetryTimeNanos = nextRetryTime;
            LOG.debug("Created {}", this);
        }

        public void setNextRetryTime() {
            nextRetryTimeNanos = nextTime(msgId);
            LOG.debug("Updated {}", this);
        }

        public boolean retry() {
            return nextRetryTimeNanos >= System.nanoTime();
        }

        @Override
        public String toString() {
            return "RetryEntry{" +
                    "msgId=" + msgId +
                    ", nextRetryTime=" + nextRetryTimeNanos +
                    '}';
        }
    }



    private static class RetryEntryTimeStampComparator implements Comparator<RetryEntry> {
        public int compare(RetryEntry entry1, RetryEntry entry2) {
            return Long.valueOf(entry1.nextRetryTimeNanos).compareTo(entry2.nextRetryTimeNanos);
        }
    }

    private static class RetryTimer {

    }

}
