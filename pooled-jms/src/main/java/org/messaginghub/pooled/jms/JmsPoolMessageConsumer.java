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

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Consumer;

import org.messaginghub.pooled.jms.internal.JmsPoolMessageConsumerProxy;

import jakarta.jms.IllegalStateException;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageListener;

/**
 * A {@link MessageConsumer} which was created by {@link JmsPoolSession}.
 */
public class JmsPoolMessageConsumer implements MessageConsumer, AutoCloseable {

    private static final AtomicIntegerFieldUpdater<JmsPoolMessageConsumer> CLOSED_UPDATER =
        AtomicIntegerFieldUpdater.newUpdater(JmsPoolMessageConsumer.class, "closed");

    private final Consumer<JmsPoolMessageConsumer> onClose;
    private final JmsPoolMessageConsumerProxy messageConsumer;
    private volatile int closed;

    /**
     * Wraps the message consumer.
     *
     * @param messageConsumer
     * 		the created consumer to wrap
     */
    JmsPoolMessageConsumer(JmsPoolMessageConsumerProxy messageConsumer, Consumer<JmsPoolMessageConsumer> onClose) {
        this.messageConsumer = messageConsumer;
        this.onClose = onClose;
    }

    @Override
    public void close() throws JMSException {
        if (CLOSED_UPDATER.compareAndSet(this, 0, 1)) {
            try {
                messageConsumer.close();
            } finally {
                onClose.accept(this);
            }
        }
    }

    @Override
    public MessageListener getMessageListener() throws JMSException {
        checkClosed();
        return messageConsumer.getMessageListener();
    }

    @Override
    public String getMessageSelector() throws JMSException {
        checkClosed();
        return messageConsumer.getMessageSelector();
    }

    @Override
    public Message receive() throws JMSException {
        checkClosed();
        return messageConsumer.receive();
    }

    @Override
    public Message receive(long timeout) throws JMSException {
        checkClosed();
        return messageConsumer.receive(timeout);
    }

    @Override
    public Message receiveNoWait() throws JMSException {
        checkClosed();
        return messageConsumer.receiveNoWait();
    }

    @Override
    public void setMessageListener(MessageListener listener) throws JMSException {
        checkClosed();
        messageConsumer.setMessageListener(listener);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " { " + messageConsumer + " }";
    }

    /**
     * Provides access to the wrapped JMS {@link MessageConsumer} and is meant primarily as a
     * test point and the application logic should not depend on this method.
     *
     * @return the wrapped JMS {@link MessageConsumer}.
     *
     * @throws JMSException if an error occurs while accessing the wrapped consumer.
     */
    MessageConsumer getProviderMessageConsumer() throws JMSException {
        checkClosed();
        return messageConsumer.getProviderMessageConsumer();
    }

    //----- Internal support methods -----------------------------------------//

    /**
     * Checks if the wrapper has been closed previously.
     *
     * @throws JMSException if the wrapper was closed.
     */
    protected void checkClosed() throws JMSException {
        if (closed != 0) {
            throw new IllegalStateException("The MessageConsumer is closed");
        }
    }

    /**
     * Returns the message consumer proxy object for subclass to access
     *
     * @return the message consumer proxy backing this wrapper.
     */
    protected JmsPoolMessageConsumerProxy getDelegate() {
        return messageConsumer;
    }
}
