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
import org.apache.storm.kafka.KafkaUnitRule;
import org.apache.storm.kafka.spout.builders.SingleTopicKafkaSpoutConfiguration;
import org.apache.storm.kafka.spout.internal.KafkaConsumerFactory;
import org.apache.storm.kafka.spout.internal.KafkaConsumerFactoryDefault;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Time;
import org.junit.Before;
import org.junit.Rule;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public abstract class KafkaSpoutAbstractTest {
    @Rule
    public KafkaUnitRule kafkaUnitRule = new KafkaUnitRule();

    @Captor
    ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> commitCapture;

    final TopologyContext topologyContext = mock(TopologyContext.class);
    final Map<String, Object> conf = new HashMap<>();
    final SpoutOutputCollector collector = mock(SpoutOutputCollector.class);
    final long commitOffsetPeriodMs = 2_000;
    final int maxRetries = 3;
    KafkaConsumer<String, String> consumerSpy;
    KafkaConsumerFactory<String, String> consumerFactory;
    KafkaSpout<String, String> spout;
    final int maxPollRecords = 10;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);

        final KafkaSpoutConfig<String, String> spoutConfig = createSpoutConfig();

        consumerSpy = spy(new KafkaConsumerFactoryDefault<String, String>().createConsumer(spoutConfig));

        consumerFactory = new KafkaConsumerFactory<String, String>() {
            @Override
            public KafkaConsumer<String, String> createConsumer(KafkaSpoutConfig<String, String> kafkaSpoutConfig) {
                return consumerSpy;
            }

        };

        spout = new KafkaSpout<>(spoutConfig, consumerFactory);
    }

    /**
     * @return {@link KafkaSpoutConfig} to be used in during test setup
     */
    abstract KafkaSpoutConfig<String, String> createSpoutConfig();

    void prepareSpout(int messageCount) throws Exception {
        SingleTopicKafkaUnitSetupHelper.populateTopicData(kafkaUnitRule.getKafkaUnit(), SingleTopicKafkaSpoutConfiguration.TOPIC, messageCount);
        SingleTopicKafkaUnitSetupHelper.initializeSpout(spout, conf, topologyContext, collector);
    }

    //TODO UPDATE JAVA DOC
    /**
     * Helper method to in sequence do:
     * <ul>
     *     <li>spout.nexTuple()</li>
     *     <li>verify messageId</li>
     *     <li>spout.ack(msgId)</li>   (if ack param) set to true)
     *     <li>reset(collector) to be able to reuse mock</li>
     * </ul>
     *
     * @param offset offset of message to be verified
     * @param ack true to invoke spout.ack(msgId)
     * @param resetCollector
     * @return {@link ArgumentCaptor} of the messageId verified
     */
    ArgumentCaptor<Object> nextTuple_verifyEmitted_ack_resetCollectorMock(int offset, boolean ack, boolean resetCollector) {
        return nextTuple_verifyEmitted_ack_resetCollectorMock(
                SingleTopicKafkaSpoutConfiguration.STREAM,
                SingleTopicKafkaSpoutConfiguration.TOPIC,
                offset,
                ack,
                resetCollector);
    }

    ArgumentCaptor<Object> nextTuple_verifyEmitted_ack_resetCollectorMock(
            String stream, String topic, int offset, boolean ack, boolean resetCollector) {

        return nextTuple_verifyEmitted_ack_resetCollectorMock(
                stream,
                new Values(topic, Integer.toString(offset), Integer.toString(offset)),
                ArgumentCaptor.forClass(Object.class),
                ack,
                resetCollector);
    }

    ArgumentCaptor<Object> nextTuple_verifyEmitted_ack_resetCollectorMock(
            String stream, List<Object> tuple, final ArgumentCaptor<Object> messageId, boolean ack, boolean resetCollector) {

        spout.nextTuple();

        verifyMessageEmitted(stream, tuple, messageId);

        if (ack) {
            spout.ack(messageId.getValue());
        }

        if (resetCollector) {
            reset(collector);
        }

        return messageId;
    }

    //TODO Clean
    ArgumentCaptor<Object> verifyMessageEmitted(int offset) {
        return verifyMessageEmitted(SingleTopicKafkaSpoutConfiguration.STREAM, SingleTopicKafkaSpoutConfiguration.TOPIC, offset);
    }

    ArgumentCaptor<Object> verifyMessageEmitted(String stream, String topic, int offset) {
        return verifyMessageEmitted(
                stream,
                new Values(topic, Integer.toString(offset), Integer.toString(offset)),
                ArgumentCaptor.forClass(Object.class));
    }

    // offset and messageId are used interchangeably
    ArgumentCaptor<Object> verifyMessageEmitted(String stream, List<Object> tuple, final ArgumentCaptor<Object> messageId) {

        verify(collector).emit(
            eq(stream),
            eq(tuple),
            messageId.capture());

        return messageId;
    }

    void commitAndVerifyAllMessagesCommitted(long msgCount) {
        commitAndVerifyAllMessagesCommitted(msgCount, 1);
    }

    void commitAndVerifyAllMessagesCommitted(long msgCount, int numTimes) {
        // reset commit timer such that commit happens on next call to nextTuple()
        Time.advanceTime(commitOffsetPeriodMs + KafkaSpout.TIMER_DELAY_MS);

        //Commit offsets
        spout.nextTuple();

        verifyAllMessagesCommitted(msgCount, numTimes);
    }

    /*
     * Asserts that commitSync has been called once,
     * that there are only commits on one topic,
     * and that the committed offset covers messageCount messages
     */
    void verifyAllMessagesCommitted(long messageCount) {
        verifyAllMessagesCommitted(messageCount, 1);
    }

    void verifyAllMessagesCommitted(long messageCount, int numTimes) {
        verify(consumerSpy, times(numTimes)).commitSync(commitCapture.capture());

        final Map<TopicPartition, OffsetAndMetadata> commits = commitCapture.getValue();
        assertThat("Expected commits for only one topic partition", commits.entrySet().size(), is(1));

        OffsetAndMetadata offset = commits.entrySet().iterator().next().getValue();
        assertThat("Expected committed offset to cover all emitted messages", offset.offset(), is(messageCount));
    }
}
