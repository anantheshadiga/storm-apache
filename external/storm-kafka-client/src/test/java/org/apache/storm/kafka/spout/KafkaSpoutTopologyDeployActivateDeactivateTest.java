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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.kafka.spout.builders.SingleTopicKafkaSpoutConfiguration;
import org.apache.storm.utils.Time;
import org.junit.Test;

import java.util.regex.Pattern;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

public class KafkaSpoutTopologyDeployActivateDeactivateTest extends KafkaSpoutAbstractTest {
    @Override
    KafkaSpoutConfig<String, String> createSpoutConfig() {
        return SingleTopicKafkaSpoutConfiguration.setCommonSpoutConfig(
            KafkaSpoutConfig.builder("127.0.0.1:" + kafkaUnitRule.getKafkaUnit().getKafkaPort(),
                Pattern.compile(SingleTopicKafkaSpoutConfiguration.TOPIC)))
            .setOffsetCommitPeriodMs(commitOffsetPeriodMs)
            .setRetry(new KafkaSpoutRetryExponentialBackoff(KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(0), KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(0),
                maxRetries, KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(0)))
            .setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.EARLIEST)
            .setProp(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords)
            .build();
    }

    @Test
    public void test_FirstPollStrategy_Earliest_NotEnforced_OnTopologyActivateDeactivate() throws Exception {
        try (Time.SimulatedTime simulatedTime = new Time.SimulatedTime()) {
            final int messageCount = 2;
            prepareSpout(messageCount);

            nextTuple_verifyEmitted_action_resetCollectorMock(0, true, true, Object.class);

            doNothing().when(consumerSpy).close();

            //Commits offsets during deactivation
            spout.deactivate();

            verifyAllMessagesCommitted(1, 1);

            spout.activate();

            nextTuple_verifyEmitted_action_resetCollectorMock(1, true, true);

            commitAndVerifyAllMessagesCommitted(messageCount, 2);
        }
    }

    @Test
    public void test_FirstPollStrategy_Earliest_NotEnforced_OnPartitionReassignment() throws Exception {
        try (Time.SimulatedTime simulatedTime = new Time.SimulatedTime()) {
            when(topologyContext.getStormId()).thenReturn("topology-1");
            
            final int messageCount = 2;
            prepareSpout(messageCount);

            nextTuple_verifyEmitted_action_resetCollectorMock(0, true, true);

            //Commits offsets during deactivation
            spout.deactivate();

            verifyAllMessagesCommitted(1, 1);

            // Restart topology with the same topology id, which mimics the behavior of partition reassignment
            setUp();
            // Initialize spout using the same populated data (i.e same kafkaUnitRule)
            SingleTopicKafkaUnitSetupHelper.initializeSpout(spout, conf, topologyContext, collector);

            nextTuple_verifyEmitted_action_resetCollectorMock(1, true, true);

            commitAndVerifyAllMessagesCommitted(messageCount, 1);
        }
    }

    @Test
    public void test_FirstPollStrategy_Earliest_Enforced_OnlyOnTopologyDeployment() throws Exception {
        try (Time.SimulatedTime simulatedTime = new Time.SimulatedTime()) {
            when(topologyContext.getStormId()).thenReturn("topology-1");

            final int messageCount = 2;
            prepareSpout(messageCount);

            nextTuple_verifyEmitted_action_resetCollectorMock(0, true, true);

            //Commits offsets during deactivation
            spout.deactivate();

            verifyAllMessagesCommitted(1, 1);

            // Restart topology with a different topology id
            setUp();
            when(topologyContext.getStormId()).thenReturn("topology-2");
            // Initialize spout using the same populated data (i.e same kafkaUnitRule)
            SingleTopicKafkaUnitSetupHelper.initializeSpout(spout, conf, topologyContext, collector);

            //Emit all messages and check that they are emitted. Ack the messages too
            for(int i = 0; i < messageCount; i++) {
                nextTuple_verifyEmitted_action_resetCollectorMock(i, true, true);
            }

            commitAndVerifyAllMessagesCommitted(messageCount, 1);
        }
    }
}
