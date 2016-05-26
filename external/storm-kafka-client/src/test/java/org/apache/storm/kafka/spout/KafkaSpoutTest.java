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

import java.util.NavigableSet;

import mockit.Deencapsulation;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import mockit.Tested;
import mockit.integration.junit4.JMockit;

@RunWith(JMockit.class)
public class KafkaSpoutTest {
    @Tested
    private KafkaSpout<String, String> kafkaSpout;
    @Injectable
    private KafkaSpoutConfig<String, String> kafkaSpoutConfig;


    static class TimerMockUp extends MockUp<Timer> {


    }

    class KafkaSpoutMock extends MockUp<KafkaSpout<String, String>> {
        @Mock
        void $init(Invocation invocation, KafkaSpoutConfig<String, String> kafkaSpoutConfig) {
            KafkaSpout<String, String> invokedInstance = invocation.getInvokedInstance();
        }

        @Mock
        private boolean poll() {
            System.out.println("Poll called");
            return false;
        }

        @Mock
        private boolean commit() {
            System.out.println("Commit called");
            return false;
        }

        @Mock
        private boolean waitingToEmit() {
            System.out.println("waitingToEmit called");
            return false;
        }



    }

    public static void main(String[] args) {
        System.out.println("bla");
    }

    @Tested KafkaSpout.OffsetEntry offsetEntry;

    private class OffsetEntryMock extends MockUp<KafkaSpout.OffsetEntry> {
        @Mock
        void $init(Invocation invocation, TopicPartition tp, long initialFetchOffset) {
            KafkaSpout.OffsetEntry entry = invocation.getInvokedInstance();
//            Deencapsulation.setField(entry, "ackedMsgs", );
        }

    }

    @Test
    public void testOffsetEntry(@Injectable final KafkaSpout<String, String> ks,
                                @Injectable TopicPartition tp,
                                @Injectable("0") long initialFetchOffset,
                                @Injectable final NavigableSet<KafkaSpoutMessageId> ackedMsgs,
                                @Injectable final KafkaSpoutMessageId kafkaSpoutMessageId) throws Exception {
        new Expectations() {{
            kafkaSpoutMessageId.offset(); returns(0, 1, 2, 4, 5);
            ackedMsgs.iterator().hasNext(); returns(true, false);
            ackedMsgs.iterator().next(); returns(kafkaSpoutMessageId);
        }};

        OffsetAndMetadata actual = offsetEntry.findNextCommitOffset();
        OffsetAndMetadata expected = new OffsetAndMetadata(2);

        Assert.assertEquals(expected, actual);
    }

    @Test
//    public void testMock(@Injectable("true") boolean initialized) {
    public void testMock() {
        KafkaSpoutMock kafkaSpout1 = new KafkaSpoutMock();

        final boolean initialized1 = Deencapsulation.getField(kafkaSpout, "initialized");
        System.out.println("initialized1 = " + initialized1);
//        Assert.assertTrue(initialized1);
        kafkaSpout.nextTuple();
    }

    @Test
    public void test() {
        System.out.println("test");
        new Expectations() {{
            Deencapsulation.setField(kafkaSpout, "initialized", true);
        }};
        int x  = 5;
        System.out.println(x);
        Deencapsulation.setField(kafkaSpout, "initialized", true);
        final boolean initialized = Deencapsulation.getField(kafkaSpout, "initialized");
        Assert.assertTrue(initialized);
    }

    @Test
    public void test1() {
        System.out.println("test1");

        new Expectations() {{
//            kafkaSpout.poll(); result = true;
        }};

        final boolean initialized = Deencapsulation.getField(kafkaSpout, "initialized");
        Assert.assertFalse(initialized);
    }

}
