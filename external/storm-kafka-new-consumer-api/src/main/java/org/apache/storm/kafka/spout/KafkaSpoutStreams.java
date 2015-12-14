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

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class KafkaSpoutStreams implements Serializable {
    private final Map<String, KafkaSpoutStream> topicToStream;

    public KafkaSpoutStreams(KafkaSpoutStream... kafkaSpoutStreams) {
        if (kafkaSpoutStreams == null || kafkaSpoutStreams.length == 0) {
            throw new IllegalArgumentException("Must specify at least one output stream");
        }

        topicToStream = new HashMap<>();
        for (KafkaSpoutStream stream : kafkaSpoutStreams) {
            topicToStream.put(stream.getTopic(), stream);
        }
    }

    public Fields getOutputFields(String topic) {
        if (topicToStream.containsKey(topic)) {
            return topicToStream.get(topic).getOutputFields();
        }
        return topicToStream.get(KafkaSpoutStream.DEFAULT_TOPIC).getOutputFields();
    }

    public String getStreamId(String topic) {
        if (topicToStream.containsKey(topic)) {
            return topicToStream.get(topic).getStreamId();
        }
        return topicToStream.get(KafkaSpoutStream.DEFAULT_TOPIC).getStreamId();
    }

    void declareOutputFields(OutputFieldsDeclarer declarer) {
        for (KafkaSpoutStream stream : topicToStream.values()) {
            declarer.declareStream(stream.getStreamId(), stream.getOutputFields());
        }
    }

    void emit(SpoutOutputCollector collector, MessageId messageId) {
        collector.emit(getStreamId(messageId.topic()), messageId.getTuple(), messageId);
    }
}
