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
package org.messaginghub.pooled.jms.internal;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import jakarta.jms.Destination;
import jakarta.jms.IllegalStateException;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageListener;
import jakarta.jms.Queue;
import jakarta.jms.QueueReceiver;
import jakarta.jms.Topic;
import jakarta.jms.TopicSubscriber;

public class JmsPoolMessageConsumerProxy implements MessageConsumer, TopicSubscriber, QueueReceiver {

    private static final AtomicIntegerFieldUpdater<JmsPoolMessageConsumerProxy> CLOSED_UPDATER =
        AtomicIntegerFieldUpdater.newUpdater(JmsPoolMessageConsumerProxy.class, "closed");

    private final MessageConsumer consumer;
    private final Destination destination;
    private final String selector;
    private final boolean noLocal;

    private volatile int closed;

    public JmsPoolMessageConsumerProxy(MessageConsumer consumer, Destination destination, String selector, boolean noLocal) {
        this.consumer = consumer;
        this.destination = destination;
        this.selector = selector;
        this.noLocal = noLocal;
    }

    public MessageConsumer getProviderMessageConsumer() throws JMSException {
        checkClosed();
        return consumer;
    }

    @Override
    public void close() throws JMSException {
        if (CLOSED_UPDATER.compareAndSet(this, 0, 1)) {
            consumer.close();
        }
    }

    @Override
    public String getMessageSelector() throws JMSException {
        checkClosed();
        return selector;
    }

    @Override
    public MessageListener getMessageListener() throws JMSException {
        checkClosed();
        return consumer.getMessageListener();
    }

    @Override
    public void setMessageListener(MessageListener listener) throws JMSException {
        checkClosed();
        consumer.setMessageListener(listener);
    }

    @Override
    public Queue getQueue() throws JMSException {
        checkClosed();
        return (Queue) destination;
    }

    @Override
    public Topic getTopic() throws JMSException {
        checkClosed();
        return (Topic) destination;
    }

    @Override
    public boolean getNoLocal() throws JMSException {
        checkClosed();
        return noLocal;
    }

    @Override
    public Message receive() throws JMSException {
        checkClosed();
        return consumer.receive();
    }

    @Override
    public Message receive(long timeout) throws JMSException {
        checkClosed();
        return consumer.receive(timeout);
    }

    @Override
    public Message receiveNoWait() throws JMSException {
        checkClosed();
        return consumer.receiveNoWait();
    }

    /**
     * Gets the state of the consumer closed flag.
     *
     * @return <code>true</code> if the consumer has been closed.
     */
    protected boolean isClosed() {
        return closed > 0;
    }

    /**
     * Checks for closure of this consumer wrapper and throws if true.
     *
     * @throws IllegalStateException if the consumer is closed.
     */
    protected void checkClosed() throws IllegalStateException {
        if (isClosed()) {
            throw new IllegalStateException("The consumer has already been closed");
        }
    }
}
