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

import org.messaginghub.pooled.jms.internal.JmsPoolMessageProducerProxy;

import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.Queue;
import jakarta.jms.QueueSender;

/**
 * A {@link QueueSender} instance that is created and managed by a pooled session.
 */
public class JmsPoolQueueSender extends JmsPoolMessageProducer implements QueueSender, AutoCloseable {

    JmsPoolQueueSender(JmsPoolMessageProducerProxy messageProducer, Queue destination, Consumer<JmsPoolMessageProducer> onClose) throws JMSException {
        super(messageProducer, destination, onClose);
    }

    @Override
    public void send(Queue queue, Message message, int deliveryMode, int priority, long timeToLine) throws JMSException {
        super.send(queue, message, deliveryMode, priority, timeToLine);
    }

    @Override
    public void send(Queue queue, Message message) throws JMSException {
        super.send(queue, message);
    }

    @Override
    public Queue getQueue() throws JMSException {
        return (Queue) getDestination();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " { " + getDelegate() + " }";
    }
}
