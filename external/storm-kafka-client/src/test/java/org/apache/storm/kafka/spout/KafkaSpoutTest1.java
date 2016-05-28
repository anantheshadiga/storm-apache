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
import java.util.TreeSet;

import mockit.Expectations;
import mockit.Injectable;
import mockit.Tested;
import mockit.integration.junit4.JMockit;

@RunWith(JMockit.class)
public class KafkaSpoutTest1 {
//    @Tested KafkaSpout.OffsetEntry offsetEntry;
    @Tested OffsetEntry offsetEntry;

/*
    private class OffsetEntryMock extends MockUp<KafkaSpout.OffsetEntry> {
        @Mock
        void $init(Invocation invocation, TopicPartition tp, long initialFetchOffset) {
            KafkaSpout.OffsetEntry entry = invocation.getInvokedInstance();
//            Deencapsulation.setField(entry, "ackedMsgs", );
        }

    }
*/

//    @Mocked NavigableSet<KafkaSpoutMessageId> ackedMsgs;

    @Test
    public void testOffsetEntry(/*@Injectable final KafkaSpout<String, String> ks,*/
                                @Injectable final TopicPartition tp,
                                @Injectable("0") final long initialFetchOffset,
                                @Injectable final NavigableSet<KafkaSpoutMessageId> ackedMsgs,
                                @Injectable final KafkaSpoutMessageId kafkaSpoutMessageId,
                                @Injectable final Iterator<KafkaSpoutMessageId> iterator) throws Exception {
        new Expectations() {{
            ackedMsgs.iterator(); result = iterator;

            kafkaSpoutMessageId.offset(); returns(0L, 1L, 2L, 3L, 4L, 5L, 9L, 10L);
            kafkaSpoutMessageId.getMetadata(Thread.currentThread()); result = "";

            iterator.hasNext(); returns(true, true, true, true, true, true, true, false);
            iterator.next(); returns(kafkaSpoutMessageId);
        }};

        OffsetAndMetadata actual = offsetEntry.findNextCommitOffset();
        OffsetAndMetadata expected = new OffsetAndMetadata(5);

        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testOffsetEntry2(/*@Injectable final KafkaSpout<String, String> ks,*/
                                @Injectable final TopicPartition tp,
                                @Injectable("0") final long initialFetchOffset,
                                @Injectable final NavigableSet<KafkaSpoutMessageId> ackedMsgs,
                                @Injectable final KafkaSpoutMessageId kafkaSpoutMessageId,
                                @Injectable final Iterator<KafkaSpoutMessageId> iterator) throws Exception {

        // offsets 0-5 have been acked, 6-8 not acked yet, and 9-10 acked out of order
        new Expectations() {{
            ackedMsgs.iterator(); result = iterator;

            kafkaSpoutMessageId.offset(); returns(0L, 1L, 2L, 3L, 4L, 5L, 9L, 10L);
            kafkaSpoutMessageId.getMetadata(Thread.currentThread()); result = "";

            iterator.hasNext(); returns(true, true, true, true, true, true, true, true, false);
            iterator.next(); returns(kafkaSpoutMessageId);
        }};

        // assert offset 5 is ready to be committed
        OffsetAndMetadata actual = offsetEntry.findNextCommitOffset();
        OffsetAndMetadata expected = new OffsetAndMetadata(5);
        Assert.assertEquals(expected, actual);

        offsetEntry.commit(actual);

        // offsets 7, 9-10 ready, but 6 not yet ready, so next offset to commit should be null
        new Expectations() {{
            ackedMsgs.iterator(); result = iterator;

            kafkaSpoutMessageId.offset(); returns(7L, 9L, 10L);

            iterator.hasNext(); returns(true, true, true, false);
            iterator.next(); returns(kafkaSpoutMessageId);
        }};

        Assert.assertNull(offsetEntry.findNextCommitOffset());

        // offsets 6-10 ready, so offset 10 is ready to be committed
        new Expectations() {{
            ackedMsgs.iterator(); result = iterator;

            kafkaSpoutMessageId.offset(); returns(6L, 7L, 8L, 9L, 10L);
            kafkaSpoutMessageId.getMetadata(Thread.currentThread()); result = "";

            iterator.hasNext(); returns(true, true, true, true, true, false);
            iterator.next(); returns(kafkaSpoutMessageId);
        }};

        actual = offsetEntry.findNextCommitOffset();
        expected = new OffsetAndMetadata(10);
        Assert.assertEquals(expected, actual);

        // after commit no offsets ready
        offsetEntry.commit(actual);
        Assert.assertNull(offsetEntry.findNextCommitOffset());
    }

    @Test
    public void testOffsetEntry1(/*@Injectable final KafkaSpout<String, String> ks,*/
                                @Injectable final TopicPartition tp,
                                @Injectable("0") final long initialFetchOffset,
                                @Injectable final TreeSet<KafkaSpoutMessageId> ackedMsgs,
                                @Injectable final KafkaSpoutMessageId kafkaSpoutMessageId,
                                @Injectable final Iterator<KafkaSpoutMessageId> iterator) throws Exception {

//        OffsetEntry offsetEntry = new OffsetEntry(tp, initialFetchOffset, ackedMsgs);

        new Expectations(ackedMsgs) {{
//            new TreeSet<>(OffsetEntry.OFFSET_COMPARATOR);
//            new TreeSet<KafkaSpoutMessageId>(OffsetEntry.OFFSET_COMPARATOR); result = ackedMsgs;
            new TreeSet<KafkaSpoutMessageId>(); result = ackedMsgs;

            ackedMsgs.iterator(); result = iterator;

            kafkaSpoutMessageId.offset(); returns(0L, 1L, 2L, 4L, 5L);
            kafkaSpoutMessageId.getMetadata(Thread.currentThread()); result = "";

            iterator.hasNext(); returns(true, true, true, true, false);
            iterator.next(); returns(kafkaSpoutMessageId);

/*            ackedMsgs.iterator().hasNext(); returns(true, false);
            ackedMsgs.iterator().next(); returns(kafkaSpoutMessageId, kafkaSpoutMessageId);*/
        }};

        OffsetAndMetadata actual = offsetEntry.findNextCommitOffset();
        OffsetAndMetadata expected = new OffsetAndMetadata(2);

        Assert.assertEquals(expected, actual);
    }
}
