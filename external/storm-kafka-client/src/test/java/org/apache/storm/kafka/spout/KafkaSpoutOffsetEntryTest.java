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

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Iterator;
import java.util.NavigableSet;

import mockit.Deencapsulation;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Tested;
import mockit.Verifications;
import mockit.integration.junit4.JMockit;

@RunWith(JMockit.class)
public class KafkaSpoutOffsetEntryTest {
    @Tested
    private KafkaSpout.OffsetEntry offsetEntry;

    @Injectable
    private KafkaSpout ks;
    @Injectable
    private TopicPartition tp;
    @Injectable("0")
    private long initialFetchOffset;
    @Injectable
    private NavigableSet<KafkaSpoutMessageId> ackedMsgs;
    @Injectable
    private KafkaSpoutMessageId kafkaSpoutMessageId;
    @Injectable
    private Iterator<KafkaSpoutMessageId> iterator;

    @Test
    public void testOffsetEntry() throws Exception {
        Deencapsulation.setField(offsetEntry, ackedMsgs);

        // offsets 0-5 have been acked, 6-8 not acked yet, and 9-10 acked out of order
        new OffsetExpectations(new Long[]{0L, 1L, 2L, 3L, 4L, 5L, 9L, 10L},
                new boolean[]{true, true, true, true, true, true, true, true, false});

        // assert offset 5 is ready to be committed
        OffsetAndMetadata actual = offsetEntry.findNextCommitOffset();
        OffsetAndMetadata expected = new OffsetAndMetadata(5);
        Assert.assertEquals(expected, actual);

        new Verifications() {{
            iterator.hasNext(); times = 7;
            iterator.next(); times = 7;
        }};

        // commit offsets 0-5
        offsetEntry.commit(actual);

        // assert committed offset is 5
        long committedOffset = Deencapsulation.getField(offsetEntry, "committedOffset");
        Assert.assertEquals(5L, committedOffset);

        // offsets 7, 9-10 ready, but 6 not yet ready, so next offset to commit should be null
        // because last committed was 5 we can only commit once 6 is ready
        new OffsetExpectations(new Long[]{7L, 9L, 10L},
                new boolean[]{true, true, true, false});

        Assert.assertNull(offsetEntry.findNextCommitOffset());

        // assert committed offset is still 5
        committedOffset = Deencapsulation.getField(offsetEntry, "committedOffset");
        Assert.assertEquals(5L, committedOffset);

        // offsets 6-10 ready, so offset 10 is ready to be committed
        new OffsetExpectations(new Long[]{6L, 7L, 8L, 9L, 10L},
                new boolean[]{true, true, true, true, true, false});

        actual = offsetEntry.findNextCommitOffset();
        expected = new OffsetAndMetadata(10);
        Assert.assertEquals(expected, actual);

        // after commit no new offsets ready
        offsetEntry.commit(actual);
        Assert.assertNull(offsetEntry.findNextCommitOffset());

        // assert committed offset is now 10
        committedOffset = Deencapsulation.getField(offsetEntry, "committedOffset");
        Assert.assertEquals(10L, committedOffset);
    }

    private final class OffsetExpectations extends Expectations {
        OffsetExpectations(Long[] offsets, boolean[] hasNext) {
            ackedMsgs.iterator(); result = iterator;

            kafkaSpoutMessageId.offset(); returns(offsets);
            kafkaSpoutMessageId.getMetadata(Thread.currentThread()); result = "";

            iterator.hasNext(); returns(hasNext);
            iterator.next(); returns(kafkaSpoutMessageId);
        }
    }
}
