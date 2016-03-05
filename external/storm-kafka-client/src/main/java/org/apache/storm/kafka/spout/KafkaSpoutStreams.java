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
import org.apache.storm.utils.Utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaSpoutStreams implements Serializable {
    private final Map<String, KafkaSpoutStream> topicToStream;

    private KafkaSpoutStreams(Builder builder) {
        this.topicToStream = builder.topicToStream;
    }

    /**
     * @param topic the topic for which to get output fields
     * @return the output fields declared
     */
    public Fields getOutputFields(String topic) {
        if (topicToStream.containsKey(topic)) {
            return topicToStream.get(topic).getOutputFields();
        }
        return topicToStream.get(KafkaSpoutStream.DEFAULT_TOPIC).getOutputFields();
    }

    /**
     * @param topic the topic to for which to get the stream id
     * @return the id of the stream to where the tuples are emitted
     */
    public String getStreamId(String topic) {
        if (topicToStream.containsKey(topic)) {
            return topicToStream.get(topic).getStreamId();
        }
        return topicToStream.get(KafkaSpoutStream.DEFAULT_TOPIC).getStreamId();
    }

    /**
     * @return list of topics subscribed and emitting tuples to a stream as configured by {@link KafkaSpoutStream}
     */
    public List<String> getTopics() {
        return new ArrayList<>(topicToStream.keySet());
    }

    void declareOutputFields(OutputFieldsDeclarer declarer) {
        for (KafkaSpoutStream stream : topicToStream.values()) {
            declarer.declareStream(stream.getStreamId(), stream.getOutputFields());
        }
    }

    void emit(SpoutOutputCollector collector, MessageId messageId) {
        collector.emit(getStreamId(messageId.topic()), messageId.getTuple(), messageId);
    }

    public static class Builder {
        private Map<String, KafkaSpoutStream> topicToStream = new HashMap<>();;

        public Builder(Fields outputFields, String... topics) {
            this(outputFields, Utils.DEFAULT_STREAM_ID, topics);
        }

        public Builder (Fields outputFields, String streamId, String... topics) {
            for (String topic : topics) {
                topicToStream.put(topic, new KafkaSpoutStream(outputFields, streamId, topic));
            }
        }

        public Builder(KafkaSpoutStream stream) {
            topicToStream.put(stream.getTopic(), stream);
        }

        public Builder addStream(KafkaSpoutStream stream) {
            topicToStream.put(stream.getTopic(), stream);
            return this;
        }

        public Builder addStream(Fields outputFields, String... topics) {
            for (String topic : topics) {
                topicToStream.put(topic, new KafkaSpoutStream(outputFields, topic));
            }
            return this;
        }

        public Builder addStream(Fields outputFields, String streamId, String... topics) {
            for (String topic : topics) {
                topicToStream.put(topic, new KafkaSpoutStream(outputFields, streamId, topic));
            }
            return this;
        }

        public KafkaSpoutStreams build() {
            return new KafkaSpoutStreams(this);
        }
    }
}
