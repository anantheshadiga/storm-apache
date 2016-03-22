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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

public class ExponentialBackoffRetry implements RetryService {
    private static final Logger LOG = LoggerFactory.getLogger(ExponentialBackoffRetry.class);
    private static final RetryEntryTimeStampComparator RETRY_ENTRY_TIME_STAMP_COMPARATOR = new RetryEntryTimeStampComparator();

    private int delay;
    private int step;
    private int maxRetries;

    private Queue<RetryEntry> entryHeap = new PriorityQueue<>(RETRY_ENTRY_TIME_STAMP_COMPARATOR);
    private Set<KafkaSpoutMessageId> entrySet = new HashSet<>();



    // nextRetry = delay + step^(retryCount - 1)


    /**
     * The next retry is scheduled according to the
     * nextRetry = delay + step^(retryCount - 1)
     * @param delay
     * @param step
     * @param maxRetries
     */
    public ExponentialBackoffRetry(int delay, int step, int maxRetries) {
        this.delay = delay;
        this.step = step;
        this.maxRetries = maxRetries;
    }

    @Override
    public boolean retry(KafkaSpoutMessageId msgId) {
        return entrySet.contains(msgId) || entryHeap.poll();
    }

    @Override()
    public void update(KafkaSpoutMessageId msgId) {

    }

    @Override
    public void remove(KafkaSpoutMessageId msgId) {
    }

    @Override
    public void add(KafkaSpoutMessageId msgId) {

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




    private class RetryEntry {
        private KafkaSpoutMessageId msgId;
        private long nextRetryTime;

        public RetryEntry(KafkaSpoutMessageId msgId) {
            this.msgId = msgId;
            nextRetryTime = System.nanoTime();
        }

        public RetryEntry(KafkaSpoutMessageId msgId, long nextRetryTime) {
            this.msgId = msgId;
            this.nextRetryTime = nextRetryTime;
        }

        public void setNextRetryTime() {
            // if value is greater than Long.MAX_VALUE it truncates to Long.MAX_VALUE
            nextRetryTime = (long)(delay + Math.pow(step, nextRetryTime));
            LOG.debug("Updated {}", this);
        }

        @Override
        public String toString() {
            return "RetryEntry{" +
                    "msgId=" + msgId +
                    ", nextRetryTime=" + nextRetryTime +
                    '}';
        }
    }



    private static class RetryEntryTimeStampComparator implements Comparator<RetryEntry> {
        public int compare(RetryEntry entry1, RetryEntry entry2) {
            return Long.valueOf(entry1.nextRetryTime).compareTo(entry2.nextRetryTime);
        }
    }

    private static class RetryTimer {

    }

}
