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

import java.util.Set;

public interface KafkaSpoutRetryService {
    /**
     * Schedule a message for retrial according to the retrial policy specified
     * @param msgId message to schedule for retrial
     */
    void schedule(KafkaSpoutMessageId msgId);


    /**
     * Removes a message from the list of messages scheduled for retrial
     * @param msgId message to remove from retrial
     */
    boolean remove(KafkaSpoutMessageId msgId);

    /**
     * @return set of topic partitions that have offsets that are ready to be retried, i.e.,
     * that failed, and whose retry time is less than current time
     */
    Set<TopicPartition> topicPartitions();


    /**
     * Checks if a specific failed {@link KafkaSpoutMessageId} is is ready to be retried,
     * i.e is scheduled and has retry time that is less than current time.
     * @return true if message is ready to be retried, false otherwise
     */
    boolean retry(KafkaSpoutMessageId msgId);
}
