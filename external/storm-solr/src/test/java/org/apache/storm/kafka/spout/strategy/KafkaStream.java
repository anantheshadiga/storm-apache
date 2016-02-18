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

package org.apache.storm.kafka.spout.strategy;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public abstract class KafkaStream<K,V> {
    private Fields outputFields;
    private String streamId;

    /** Declare specified outputFields using default stream */
    public KafkaStream(Fields outputFields) {
        this(outputFields, Utils.DEFAULT_STREAM_ID);
    }

    public KafkaStream(Fields outputFields, String streamId) {
        this.outputFields = outputFields;
        this.streamId = streamId;
    }

    public abstract Values buildTuple(TopicPartition topicPartition, ConsumerRecord<K,V> consumerRecord);

    public Fields getOutputFields() {
        return outputFields;
    }

    public String getStreamId() {
        return streamId;
    }
}
