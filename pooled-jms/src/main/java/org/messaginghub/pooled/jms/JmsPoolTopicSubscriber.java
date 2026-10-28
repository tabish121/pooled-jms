/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.messaginghub.pooled.jms;

import java.util.function.Consumer;

import org.messaginghub.pooled.jms.internal.JmsMessageConsumerProxy;

import jakarta.jms.JMSException;
import jakarta.jms.Topic;
import jakarta.jms.TopicSubscriber;

/**
 * A {@link TopicSubscriber} which was created by {@link JmsPoolSession}.
 */
public class JmsPoolTopicSubscriber extends JmsPoolMessageConsumer implements TopicSubscriber, AutoCloseable {

    JmsPoolTopicSubscriber(JmsMessageConsumerProxy delegate, Topic destination, Consumer<JmsPoolMessageConsumer> onClose) {
        super(delegate, destination, onClose);
    }

    @Override
    public Topic getTopic() throws JMSException {
        return getProxy().getTopic();
    }

    @Override
    public boolean getNoLocal() throws JMSException {
        return getProxy().getNoLocal();
    }
}
