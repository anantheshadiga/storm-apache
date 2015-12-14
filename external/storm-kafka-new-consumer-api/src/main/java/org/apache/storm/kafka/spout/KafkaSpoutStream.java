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

import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import java.io.Serializable;

public class KafkaSpoutStream implements Serializable {
    public static final String DEFAULT_TOPIC = "DEFAULT_TOPIC_KAFKA_SPOUT_INTERNAL";
    private final Fields outputFields;
    private final String streamId;
    private final String topic;

    /** Declare specified outputFields with default stream and default topic */
    public KafkaSpoutStream(Fields outputFields) {
        this(outputFields, Utils.DEFAULT_STREAM_ID);
    }

    /** Declare specified outputFields with specified stream and default topic */
    public KafkaSpoutStream(Fields outputFields, String streamId) {
        this(outputFields, streamId, DEFAULT_TOPIC);
    }

    /** Declare specified outputFields with specified stream and specified topic */
    public KafkaSpoutStream(Fields outputFields, String streamId, String topic) {
        this.outputFields = outputFields;
        this.streamId = streamId;
        this.topic = topic;
    }

    public Fields getOutputFields() {
        return outputFields;
    }

    public String getStreamId() {
        return streamId;
    }

    public String getTopic() {
        return topic;
    }
}
