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

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

public class KafkaSpoutExponentialBackoffRetry implements KafkaSpoutRetryService {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSpoutExponentialBackoffRetry.class);
    private static final RetryEntryTimeStampComparator RETRY_ENTRY_TIME_STAMP_COMPARATOR = new RetryEntryTimeStampComparator();

    // nextRetry = delay + ratio^(failCount - 1)
    private Delay delay;
    private int ratio;
    private int maxRetries;
    private Delay maxRetryDelay;

    private Set<RetrySchedule> retrySchedules = new TreeSet<>(RETRY_ENTRY_TIME_STAMP_COMPARATOR);
    private Set<KafkaSpoutMessageId> toRetryMsgs = new HashSet<>();      // Convenience data structure to speedup lookups

    /**
     * Comparator ordering by timestamp 
     */
    private static class RetryEntryTimeStampComparator implements Comparator<RetrySchedule> {
        public int compare(RetrySchedule entry1, RetrySchedule entry2) {
            return Long.valueOf(entry1.nextRetryTimeNanos()).compareTo(entry2.nextRetryTimeNanos());
        }
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

        public boolean retry(long currentTimeNanos) {
            return nextRetryTimeNanos <= currentTimeNanos;
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

        @Override
        public String toString() {
            return "Delay{" +
                    "delay=" + delay +
                    ", timeUnit=" + timeUnit +
                    '}';
        }
    }

    /**
     * The time stamp of the next retry is scheduled according to the exponential backoff formula (shifted geometric progression):
     * nextRetry = failCount == 1 ? System.nanoTime() + delay + ratio^(failCount-1) : System.nanoTime() + ratio^(failCount-1)
     * nextRetry = Max(nextRetry, maxRetryDelay)
     *
     * @param delay      initial delay
     * @param ratio      ratio of the geometric progression
     * @param maxRetries maximum number of times a tuple is retried before being acked and set for commit
     * @param maxRetryDelay maximum amount of time waiting before retrying
     *
     */
    public KafkaSpoutExponentialBackoffRetry(Delay delay, int ratio, int maxRetries, Delay maxRetryDelay) {
        this.delay = delay;
        this.ratio = ratio;
        this.maxRetries = maxRetries;
        this.maxRetryDelay = maxRetryDelay;
        LOG.debug("Created {}", this);
    }

    @Override
    public Set<TopicPartition> topicPartitions() {
        final Set<TopicPartition> tps = new TreeSet<>();
        final long currentTimeNanos = System.nanoTime();
        for (RetrySchedule retrySchedule : retrySchedules) {
            if (retrySchedule.nextRetryTimeNanos <  currentTimeNanos) {
                final KafkaSpoutMessageId msgId = retrySchedule.msgId;
                tps.add(new TopicPartition(msgId.topic(), msgId.partition()));
            } else {
                break;  // Stop search as soon passed current time
            }
        }
        LOG.debug("Topic partitions with entries ready to be retried [{}] ", tps);
        return tps;
    }

    @Override
    public boolean retry(KafkaSpoutMessageId msgId) {
        boolean retry = false;
        if (toRetryMsgs.contains(msgId)) {
            final long currentTimeNanos = System.nanoTime();
            for (RetrySchedule retrySchedule : retrySchedules) {
                if (retrySchedule.retry(currentTimeNanos)) {
                    if (retrySchedule.msgId.equals(msgId)) {
                        retry = true;
                        LOG.debug("Found entry to retry {}", retrySchedule);
                    }
                } else {
                    LOG.debug("Entry to retry not found {}", retrySchedule);
                    break;
                }
            }
        }
        return retry;
    }

    @Override
    public boolean remove(KafkaSpoutMessageId msgId) {
        if (toRetryMsgs.contains(msgId)) {
            for (Iterator<RetrySchedule> iterator = retrySchedules.iterator(); iterator.hasNext(); ) {
                final RetrySchedule retrySchedule = iterator.next();
                if (retrySchedule.msgId().equals(msgId)) {
                    iterator.remove();
                    toRetryMsgs.remove(msgId);
                    LOG.debug("Removed {}", retrySchedule);
                    LOG.trace("Current state {}", retrySchedules);
                    return true;
                }
            }
        }
        LOG.debug("Not found {}", msgId);
        LOG.trace("Current state {}", retrySchedules);
        return false;
    }

    /**
     * Schedules this {@link KafkaSpoutMessageId} if not yet scheduled, or updates retry time if it has already been scheduled.
     */
    @Override
    public void schedule(KafkaSpoutMessageId msgId) {
        if (msgId.numFails() > maxRetries) {
            LOG.debug("Not scheduling [{}] because reached maximum number of retries [{}].", msgId, maxRetries);
        } else {
            if (toRetryMsgs.contains(msgId)) {
                for (Iterator<RetrySchedule> iterator = retrySchedules.iterator(); iterator.hasNext(); ) {
                    final RetrySchedule retrySchedule = iterator.next();
                    if (retrySchedule.msgId().equals(msgId)) {
                        iterator.remove();
                        toRetryMsgs.remove(msgId);
                    }
                }
            }
            final RetrySchedule retrySchedule = new RetrySchedule(msgId, nextTime(msgId));
            retrySchedules.add(retrySchedule);
            toRetryMsgs.add(msgId);
            LOG.debug("Scheduled. {}", retrySchedule);
            LOG.trace("Current state {}", retrySchedules);
        }
    }

    // if value is greater than Long.MAX_VALUE it truncates to Long.MAX_VALUE
    private long nextTime(KafkaSpoutMessageId msgId) {
        final long nextTimeNanos = msgId.numFails() == 1
                ? (long) (System.nanoTime() + delay.delayNanos() + Math.pow(ratio, msgId.numFails() - 1))
                : (long) (System.nanoTime() + Math.pow(ratio, msgId.numFails() - 1));
        return Math.min(nextTimeNanos, maxRetryDelay.delayNanos);
    }

    @Override
    public String toString() {
        return "KafkaSpoutExponentialBackoffRetry{" +
                "delay=" + delay +
                ", ratio=" + ratio +
                ", maxRetries=" + maxRetries +
                ", maxRetryDelay=" + maxRetryDelay +
                '}';
    }
}
