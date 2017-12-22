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
import org.apache.storm.kafka.spout.builders.SingleTopicKafkaSpoutConfiguration;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Map;

import static org.apache.storm.kafka.spout.KafkaSpoutAbstractTest.Action.*;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.kafka.common.TopicPartition;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Time.SimulatedTime;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyString;

import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.hamcrest.Matchers;

public class KafkaSpoutSingleTopicTest extends KafkaSpoutAbstractTest {
    @Override
    KafkaSpoutConfig<String, String> createSpoutConfig() {
        return SingleTopicKafkaSpoutConfiguration.setCommonSpoutConfig(
            KafkaSpoutConfig.builder("127.0.0.1:" + kafkaUnitRule.getKafkaUnit().getKafkaPort(),
                Pattern.compile(SingleTopicKafkaSpoutConfiguration.TOPIC)))
            .setOffsetCommitPeriodMs(commitOffsetPeriodMs)
            .setRetry(new KafkaSpoutRetryExponentialBackoff(KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(0), KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(0),
                maxRetries, KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(0)))
            .setProp(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords)
            .build();
    }

    @Test
    public void testSeekToCommittedOffsetIfConsumerPositionIsBehindWhenCommitting() throws Exception {
        try (SimulatedTime simulatedTime = new SimulatedTime()) {
            int messageCount = maxPollRecords * 2;
            prepareSpout(messageCount);

            //Emit all messages and fail the first one while acking the rest
            for (int i = 0; i < messageCount; i++) {
                spout.nextTuple();
            }

            final ArgumentCaptor<KafkaSpoutMessageId> messageIdCaptor = verifyMessageEmitted(
                SingleTopicKafkaSpoutConfiguration.STREAM, anyListOf(Object.class), KafkaSpoutMessageId.class, messageCount -1);

            List<KafkaSpoutMessageId> messageIds = messageIdCaptor.getAllValues();
            for (int i = 1; i < messageIds.size(); i++) {
                spout.ack(messageIds.get(i));
            }

            KafkaSpoutMessageId failedTuple = messageIds.get(0);
            spout.fail(failedTuple);

            //Advance the time and replay the failed tuple. 
            reset(collector);

            ArgumentCaptor<KafkaSpoutMessageId> failedIdReplayCaptor = nextTuple_verifyEmitted_action_resetCollectorMock(
                anyString(), anyListOf(Object.class), KafkaSpoutMessageId.class, NONE, false);

            assertThat("Expected replay of failed tuple", failedIdReplayCaptor.getValue(), is(failedTuple));

            /* Ack the tuple, and commit.
             * Since the tuple is more than max poll records behind the most recent emitted tuple, the consumer won't catch up in this poll.
             */
            Time.advanceTime(KafkaSpout.TIMER_DELAY_MS + commitOffsetPeriodMs);
            spout.ack(failedIdReplayCaptor.getValue());
            spout.nextTuple();
            verify(consumerSpy).commitSync(commitCapture.capture());
            
            Map<TopicPartition, OffsetAndMetadata> capturedCommit = commitCapture.getValue();
            TopicPartition expectedTp = new TopicPartition(SingleTopicKafkaSpoutConfiguration.TOPIC, 0);
            assertThat("Should have committed to the right topic", capturedCommit, Matchers.hasKey(expectedTp));
            assertThat("Should have committed all the acked messages", capturedCommit.get(expectedTp).offset(), is((long)messageCount));

            /* Verify that the following acked (now committed) tuples are not emitted again
             * Since the consumer position was somewhere in the middle of the acked tuples when the commit happened,
             * this verifies that the spout keeps the consumer position ahead of the committed offset when committing
             */
            reset(collector);
            //Just do a few polls to check that nothing more is emitted
            for(int i = 0; i < 3; i++) {
                spout.nextTuple();
            }
            verify(collector, never()).emit(anyString(), anyList(), anyObject());
        }
    }

    @Test
    public void testShouldContinueWithSlowDoubleAcks() throws Exception {
        try (SimulatedTime simulatedTime = new SimulatedTime()) {
            int messageCount = 20;
            prepareSpout(messageCount);

            //play 1st tuple
            ArgumentCaptor<Object> messageIdToDoubleAck = nextTuple_verifyEmitted_action_resetCollectorMock(
                anyString(), anyListOf(Object.class), Object.class, ACK, false);

            //Emit some more messages
            for(int i = 0; i < messageCount / 2; i++) {
                spout.nextTuple();
            }

            spout.ack(messageIdToDoubleAck.getValue());

            //Emit any remaining messages
            for(int i = 0; i < messageCount; i++) {
                spout.nextTuple();
            }

            //Verify that all messages are emitted, ack all the messages
            ArgumentCaptor<Object> messageIds = verifyMessageEmitted(
                    SingleTopicKafkaSpoutConfiguration.STREAM, anyListOf(Object.class), Object.class, messageCount);

            for(Object id : messageIds.getAllValues()) {
                spout.ack(id);
            }

            commitAndVerifyAllMessagesCommitted(messageCount);
        }
    }

    @Test
    public void testShouldEmitAllMessages() throws Exception {
        try (SimulatedTime simulatedTime = new SimulatedTime()) {
            int messageCount = 10;
            prepareSpout(messageCount);

            //Emit all messages and check that they are emitted. Ack the messages too
            for(int i = 0; i < messageCount; i++) {
                nextTuple_verifyEmitted_action_resetCollectorMock(i, ACK, Object.class, true);
            }

            commitAndVerifyAllMessagesCommitted(messageCount);
        }
    }

    @Test
    public void testShouldReplayInOrderFailedMessages() throws Exception {
        try (SimulatedTime simulatedTime = new SimulatedTime()) {
            int messageCount = 10;
            prepareSpout(messageCount);

            //play and ack 1 tuple
            nextTuple_verifyEmitted_action_resetCollectorMock(
                anyString(), anyListOf(Object.class), Object.class, ACK, true);

            //play and fail 1 tuple
            nextTuple_verifyEmitted_action_resetCollectorMock(
                anyString(), anyListOf(Object.class), Object.class, FAIL, true);

            //Emit all remaining messages. Failed tuples retry immediately with current configuration, so no need to wait.
            for(int i = 0; i < messageCount; i++) {
                spout.nextTuple();
            }

            //All messages except the first acked message should have been emitted
            ArgumentCaptor<Object> remainingMessageIds = verifyMessageEmitted(
                    SingleTopicKafkaSpoutConfiguration.STREAM, anyListOf(Object.class), Object.class, messageCount -1);

            for(Object id : remainingMessageIds.getAllValues()) {
                spout.ack(id);
            }

            commitAndVerifyAllMessagesCommitted(messageCount);
        }
    }

    @Test
    public void testShouldReplayFirstTupleFailedOutOfOrder() throws Exception {
        try (SimulatedTime simulatedTime = new SimulatedTime()) {
            int messageCount = 10;
            prepareSpout(messageCount);

            //play 1st tuple
            ArgumentCaptor<Object> messageIdToFail = nextTuple_verifyEmitted_action_resetCollectorMock(
                anyString(), anyListOf(Object.class), Object.class, NONE, true);

            //play 2nd tuple
            ArgumentCaptor<Object> messageIdToAck = nextTuple_verifyEmitted_action_resetCollectorMock(
                anyString(), anyListOf(Object.class), Object.class, NONE, true);

            //ack 2nd tuple
            spout.ack(messageIdToAck.getValue());
            //fail 1st tuple
            spout.fail(messageIdToFail.getValue());

            //Emit all remaining messages. Failed tuples retry immediately with current configuration, so no need to wait.
            for(int i = 0; i < messageCount; i++) {
                spout.nextTuple();
            }

            //All messages except the first acked message should have been emitted
            final ArgumentCaptor<Object> remainingIds = verifyMessageEmitted(
                SingleTopicKafkaSpoutConfiguration.STREAM, anyListOf(Object.class), Object.class, messageCount -1);

            for(Object id : remainingIds.getAllValues()) {
                spout.ack(id);
            }

            commitAndVerifyAllMessagesCommitted(messageCount);
        }
    }

    @Test
    public void testShouldReplayAllFailedTuplesWhenFailedOutOfOrder() throws Exception {
        //The spout must re-emit retriable tuples, even if they fail out of order.
        //The spout should be able to skip tuples it has already emitted when retrying messages, even if those tuples are also retries.
        final int messageCount = 10;
        prepareSpout(messageCount);

        //play all tuples
        for (int i = 0; i < messageCount; i++) {
            spout.nextTuple();
        }

        final ArgumentCaptor<KafkaSpoutMessageId> messageIds =
            verifyMessageEmitted(anyString(), anyListOf(Object.class), KafkaSpoutMessageId.class, messageCount);

        reset(collector);

        //Fail tuple 5 and 3, call nextTuple, then fail tuple 2
        List<KafkaSpoutMessageId> capturedMessageIds = messageIds.getAllValues();
        spout.fail(capturedMessageIds.get(5));
        spout.fail(capturedMessageIds.get(3));
        spout.nextTuple();
        spout.fail(capturedMessageIds.get(2));

        //Check that the spout will reemit all 3 failed tuples and no other tuples
        ArgumentCaptor<KafkaSpoutMessageId> reemittedMessageIds = ArgumentCaptor.forClass(KafkaSpoutMessageId.class);
        for (int i = 0; i < messageCount; i++) {
            spout.nextTuple();
        }
        verify(collector, times(3)).emit(anyString(), anyList(), reemittedMessageIds.capture());

        final Set<KafkaSpoutMessageId> expectedReemitIds = new HashSet<>();
        expectedReemitIds.add(capturedMessageIds.get(5));
        expectedReemitIds.add(capturedMessageIds.get(3));
        expectedReemitIds.add(capturedMessageIds.get(2));

        assertThat("Expected re-emits to be the 3 failed tuples", new HashSet<>(reemittedMessageIds.getAllValues()), is(expectedReemitIds));
    }

    @Test
    public void testShouldDropMessagesAfterMaxRetriesAreReached() throws Exception {
        //Check that if one message fails repeatedly, the retry cap limits how many times the message can be reemitted
        int messageCount = 1;
        prepareSpout(messageCount);

        //Emit and fail the same tuple until we've reached retry limit
        for (int i = 0; i <= maxRetries; i++) {
            final ArgumentCaptor<KafkaSpoutMessageId> messageIdFailed = nextTuple_verifyEmitted_action_resetCollectorMock(
                    anyString(), anyListOf(Object.class), KafkaSpoutMessageId.class, ACK, true);

            final KafkaSpoutMessageId msgId = messageIdFailed.getValue();

            assertThat("Expected message id number of failures to match the number of times the message has failed",
                msgId.numFails(), is(i + 1));
        }

        //Verify that the tuple is not emitted again
        spout.nextTuple();
        verify(collector, never()).emit(anyString(), anyListOf(Object.class), anyObject());
    }

    @Test
    public void testSpoutMustRefreshPartitionsEvenIfNotPolling() throws Exception {
        try (SimulatedTime time = new SimulatedTime()) {
            SingleTopicKafkaUnitSetupHelper.initializeSpout(spout, conf, topologyContext, collector);

            //Nothing is assigned yet, should emit nothing
            spout.nextTuple();
            verify(collector, never()).emit(anyString(), anyList(), any(KafkaSpoutMessageId.class));

            SingleTopicKafkaUnitSetupHelper.populateTopicData(kafkaUnitRule.getKafkaUnit(), SingleTopicKafkaSpoutConfiguration.TOPIC, 1);

            Time.advanceTime(KafkaSpoutConfig.DEFAULT_PARTITION_REFRESH_PERIOD_MS + KafkaSpout.TIMER_DELAY_MS);

            //The new partition should be discovered and the message should be emitted
            spout.nextTuple();
            verify(collector).emit(anyString(), anyList(), any(KafkaSpoutMessageId.class));
        }
    }
}
