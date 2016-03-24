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
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

public class ExponentialBackoffRetry implements RetryService {
    private static final Logger LOG = LoggerFactory.getLogger(ExponentialBackoffRetry.class);
    private static final RetryEntryTimeStampComparator RETRY_ENTRY_TIME_STAMP_COMPARATOR = new RetryEntryTimeStampComparator();
    private static final MessageIdTopicPartitionComparator MESSAGE_ID_TOPIC_PARTITION_COMPARATOR = new MessageIdTopicPartitionComparator();

    // nextRetry = delay + step^(retryCount - 1)
    private Delay delay;
    private int ratio;
    private int maxRetries;

//    private Queue<RetrySchedule> entryHeap = new PriorityQueue<>(RETRY_ENTRY_TIME_STAMP_COMPARATOR);
    private Set<RetrySchedule> retrySchedules = new TreeSet<>(RETRY_ENTRY_TIME_STAMP_COMPARATOR);   //
    private Set<KafkaSpoutMessageId> failedMsgs = new HashSet<>();  // Messages that have failed

    /**
     * Comparator ordering by timestamp 
     */
    private static class RetryEntryTimeStampComparator implements Comparator<RetrySchedule> {
        public int compare(RetrySchedule entry1, RetrySchedule entry2) {
            return Long.valueOf(entry1.nextRetryTimeNanos()).compareTo(entry2.nextRetryTimeNanos());
        }
    }

    /**
     * Comparator ordering by topic first and then per partition number 
     */
    private static class MessageIdTopicPartitionComparator implements Comparator<KafkaSpoutMessageId> {
        public int compare(KafkaSpoutMessageId msg1, KafkaSpoutMessageId msg2) {
            if (msg1.topic().compareTo(msg2.topic()) < 0)
                return -1;
            if (msg1.topic().compareTo(msg2.topic()) > 0)
                return 1;
            return msg1.partition() - msg2.partition();
        }
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

    /**
     * The time stamp of the next retry is scheduled according to the formula (shifted geometric progression): nextRetry =
     * retryCount == 0 ? System.nanoTime() + delay + ratio^(retryCount) : System.nanoTime() + ratio^(retryCount) Note: retryCount
     * and failCount
     *
     * @param delay      initial delay
     * @param ratio      ratio of the geometric progression
     * @param maxRetries maximum number a tuple is retried before being set for commit
     */
    public ExponentialBackoffRetry(Delay delay, int ratio, int maxRetries) {
        this.delay = delay;
        this.ratio = ratio;
        this.maxRetries = maxRetries;
    }


    /**
     * @return One KafkaSpoutMessageId per topic partition that ais ready to be retried, i.e. exponential backoff has expired
     */
    @Override
    public Set<KafkaSpoutMessageId> next() {
        final Set<KafkaSpoutMessageId> result = new TreeSet<>(MESSAGE_ID_TOPIC_PARTITION_COMPARATOR);
        final long currentTime = System.nanoTime();
        for (RetrySchedule retrySchedule : retrySchedules) {
            if (retrySchedule.nextRetryTimeNanos() <  currentTime) {
                result.add(retrySchedule.msgId);
            } else {
                break;
            }
        }
        LOG.debug("Next retry [{}] ", result);
        return result;
    }

    @Override
    public Set<TopicPartition> topicPartitions() {
        final Set<TopicPartition> result = new TreeSet<>();
        final long currentTime = System.nanoTime();
        for (RetrySchedule retrySchedule : retrySchedules) {
            if (retrySchedule.nextRetryTimeNanos() <  currentTime) {
                final KafkaSpoutMessageId msgId = retrySchedule.msgId;
                result.add(new TopicPartition(msgId.topic(), msgId.partition()));
            } else {
                break;
            }
        }
        LOG.debug("Topic partitions affected in next retry [{}] ", result);
        return result;
    }

    @Override
    public boolean retry(KafkaSpoutMessageId msgId) {
        final long currentTime = System.nanoTime();
        for (RetrySchedule retrySchedule : retrySchedules) {
            if (retrySchedule.nextRetryTimeNanos <  currentTime) {
                if (retrySchedule.msgId.equals(msgId)) {
                    LOG.debug("Found entry to retry {}", retrySchedule);
                }
            } else {
                LOG.debug("Entry to retry not found {}", retrySchedule);
                break;
            }
        }

    }


    @Override
    public boolean update(KafkaSpoutMessageId msgId) {
        boolean updated;
        if (failedMsgs.contains(msgId)) {
            for (RetrySchedule retrySchedule : this.retrySchedules) {
                if (retrySchedule.msgId.equals(msgId)) {
                    retrySchedule.setNextRetryTime();
                    updated = true;
                }
            }
        }

    }

    public <K, V> boolean update(ConsumerRecords<K, V> consumerRecords) {
        Map<TopicPartition, Long> tpToLargestFetchedOffset = new HashMap<>();
        for (TopicPartition tp : consumerRecords.partitions()) {
            final List<ConsumerRecord<K, V>> records = consumerRecords.records(tp);
            tpToLargestFetchedOffset.put(tp, records.get(records.size() - 1).offset());
        }


        for (ConsumerRecord<K, V> record : consumerRecords) {
            if (failedMsgs.contains(new KafkaSpoutMessageId(record)) &&)
        }
        boolean updated;
        if (failedMsgs.contains(msgId)) {
            for (RetrySchedule retrySchedule : this.retrySchedules) {
                if (retrySchedule.msgId.equals(msgId)) {
                    retrySchedule.setNextRetryTime();
                    updated = true;
                }
            }
        }

    }

    @Override
    public boolean remove(KafkaSpoutMessageId msgId) {
        boolean exists = false;
        if (failedMsgs.contains(msgId)) {
            for (Iterator<RetrySchedule> iterator = retrySchedules.iterator(); iterator.hasNext(); ) {
                final RetrySchedule retrySchedule = iterator.next();
                if (retrySchedule.msgId().equals(msgId)) {
                    iterator.remove();
                    exists = true;
                    LOG.debug("Removed {}", retrySchedule);
                    break;
                }
            }
        }
        return exists;
    }

    /**
     * Adds if it is the first time. Updates retry time if it has already been scheduled.
     */
    @Override
    public void schedule(KafkaSpoutMessageId msgId) {
        if (msgId.numFails() > maxRetries) {
            LOG.debug("Not scheduling retry number [{}] because reached maximum number of retries [{}].",
                    msgId.numFails(), maxRetries);
        } else {
            if (failedMsgs.contains(msgId)) {
                for (Iterator<RetrySchedule> iterator = retrySchedules.iterator(); iterator.hasNext(); ) {
                    final RetrySchedule retrySchedule = iterator.next();
                    if (retrySchedule.msgId().equals(msgId)) {
                        iterator.remove();
                    }
                }
            }
            final RetrySchedule retrySchedule = new RetrySchedule(msgId, nextTime(msgId));
            retrySchedules.add(retrySchedule);
            LOG.debug("{}", retrySchedule);
        }
    }

    public static class RetryEntries {
        private RetryEntryTimeStampComparator RETRY_ENTRY_TIME_STAMP_COMPARATOR = new RetryEntryTimeStampComparator();
        private Queue<RetrySchedule> entryHeap = new PriorityQueue<>(RETRY_ENTRY_TIME_STAMP_COMPARATOR);
        private Set<RetrySchedule> entrySet = new HashSet<>();

        public void add(RetrySchedule retrySchedule) {
            entryHeap.add(retrySchedule);
            entrySet.add(retrySchedule);
        }

        public boolean remove(RetrySchedule retrySchedule) {
            entryHeap.remove(retrySchedule);
            return entrySet.remove(retrySchedule);
        }

        public boolean contains(RetrySchedule retrySchedule) {
            return entrySet.contains(retrySchedule);
        }

        public RetrySchedule first() {
            return entryHeap.peek();
        }
    }

    // if value is greater than Long.MAX_VALUE it truncates to Long.MAX_VALUE
    private long nextTime(KafkaSpoutMessageId msgId) {
        return msgId.numFails() == 1
                ? (long) (System.nanoTime() + delay.delayNanos() + Math.pow(ratio, msgId.numFails() - 1))
                : (long) (System.nanoTime() + Math.pow(ratio, msgId.numFails() - 1));
    }


    private class RetrySchedule {
        private KafkaSpoutMessageId msgId;
        private long nextRetryTimeNanos;

        public RetrySchedule(KafkaSpoutMessageId msgId, long nextRetryTime) {
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
            return "RetrySchedule{" +
                    "msgId=" + msgId +
                    ", nextRetryTime=" + nextRetryTimeNanos +
                    '}';
        }

        public KafkaSpoutMessageId msgId() {
            return msgId;
        }

        public long nextRetryTimeNanos() {
            return nextRetryTimeNanos;
        }
    }
}
