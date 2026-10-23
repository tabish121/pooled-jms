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

import java.lang.invoke.MethodHandles;
import java.util.function.BiConsumer;

import org.messaginghub.pooled.jms.util.Referenced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.jms.CompletionListener;
import jakarta.jms.Destination;
import jakarta.jms.IllegalStateException;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.QueueSender;
import jakarta.jms.Topic;
import jakarta.jms.TopicPublisher;

/**
 * JMS MessageProducer proxy instance
 */
public class JmsMessageProducerProxy implements MessageProducer, TopicPublisher, QueueSender {

    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final JmsAbstractSessionProxy<?> session;
    private final MessageProducer producer;
    private final Destination destination;
    private final BiConsumer<JmsMessageProducerProxy, Destination> onClose;
    private final BiConsumer<JmsMessageProducerProxy, Destination> onDestroy;
    private final Referenced referenced;

    private volatile boolean closed;

    JmsMessageProducerProxy(JmsAbstractSessionProxy<?> session,
                            MessageProducer producer,
                            Destination destination,
                            Referenced referenced,
                            BiConsumer<JmsMessageProducerProxy, Destination> onClose,
                            BiConsumer<JmsMessageProducerProxy, Destination> onDestroy) {
        this.session = session;
        this.producer = producer;
        this.onClose = onClose;
        this.onDestroy = onDestroy;
        this.destination = destination;
        this.referenced = referenced;
    }

    /**
     * Attempts to take a new reference out on this message producer proxy
     *
     * @return this message producer proxy
     *
     * @throws IllegalStateException if the message producer proxy is already closed
     */
    public synchronized JmsMessageProducerProxy acquire() throws IllegalStateException {
        checkClosed();
        referenced.acquire();
        return this;
    }

    /**
     * Called to force the JMS message producer and this proxy to be closed and not be served
     * again to any callers who create producers from the parent session.
     */
    public synchronized void destroy() {
        if (!closed) {
            closed = true;

            try {
                producer.close();
            } catch (JMSException ex) {
                LOG.debug("Suppressed error from close of wrapped JMS MessageProducer.", ex);
            }

            try {
                onDestroy.accept(this, destination);
            } catch (Exception ex) {
                LOG.debug("Suppressed error from on destroy handler in producer proy.", ex);
            }
        }
    }

    @Override
    public synchronized void close() {
        if (checkIfProducerSilentlyClosed()) {
            destroy();
        } else if (referenced.release() && !closed) {
            closed = true;

            try {
                producer.close();
            } catch (JMSException ex) {
                LOG.debug("Suppressed error from close of wrapped JMS MessageProducer.", ex);
            }

            try {
                onClose.accept(this, destination);
            } catch (Exception ex) {
                LOG.debug("Suppressed error from on close handler in producer proy.", ex);
            }
        }
    }

    /**
     * {@return if the JMS provider support delivery delay for its producers}
     */
    public boolean isDelayedDeliverySupported() {
        return session.getVersionSupport().isDelayedDeliverySupported();
    }

    /**
     * Enforces that the producer does provide delivery delay support
     *
     * @throws JMSException if delivery delay is not supported by the provider
     */
    public void enforceDelayedDeliverySupport() throws JMSException {
        session.getVersionSupport().enforceDelayedDeliverySupport();
    }

    /**
     * Enforces that the producer does provide JMS completion listener support
     *
     * @throws JMSException if completion listeners are not supported by the provider
     */
    public void enforceCompletionListenerSupport() throws JMSException {
        session.getVersionSupport().enforceCompletionListenerSupport();
    }

    /**
     * Returns the proxied JMS message message producer instance if not already closed
     *
     * @return the proxied JMS message message producer instance if not already closed
     *
     * @throws JMSException if the message producer is already closed
     */
    public MessageProducer getProviderMessageProducer() throws JMSException {
        checkClosed();
        return producer;
    }

    /**
     * Gets the JMS destination that this producer was created for which can be null
     * if this is an anonymous producer.
     *
     * @return the JMS destination this producer send messages to
     *
     * @throws JMSException if the producer is already closed
     */
    public Destination getProviderDestination() throws JMSException {
        checkClosed();
        return destination;
    }

    /**
     * Returns <code>true</code> if the producer is not assigned a fixed destination and
     * can be used to send to different destinations assigned a send method that accepts
     * the target of the send.
     *
     * @return <code>true</code> if this producer has no assigned destination.
     */
    public boolean isAnonymousProducer() {
        return destination == null;
    }

    @Override
    public void setDisableMessageID(boolean value) throws JMSException {
        checkClosed();
        producer.setDisableMessageID(value);
    }

    @Override
    public boolean getDisableMessageID() throws JMSException {
        checkClosed();
        return producer.getDisableMessageID();
    }

    @Override
    public void setDisableMessageTimestamp(boolean value) throws JMSException {
        checkClosed();
        producer.setDisableMessageTimestamp(value);
    }

    @Override
    public boolean getDisableMessageTimestamp() throws JMSException {
        checkClosed();
        return producer.getDisableMessageTimestamp();
    }

    @Override
    public void setDeliveryMode(int deliveryMode) throws JMSException {
        checkClosed();
        producer.setDeliveryMode(deliveryMode);
    }

    @Override
    public int getDeliveryMode() throws JMSException {
        checkClosed();
        return producer.getDeliveryMode();
    }

    @Override
    public void setPriority(int priority) throws JMSException {
        checkClosed();
        producer.setPriority(priority);
    }

    @Override
    public int getPriority() throws JMSException {
        checkClosed();
        return producer.getPriority();
    }

    @Override
    public void setTimeToLive(long timeToLive) throws JMSException {
        checkClosed();
        producer.setTimeToLive(timeToLive);
    }

    @Override
    public long getTimeToLive() throws JMSException {
        checkClosed();
        return producer.getTimeToLive();
    }

    @Override
    public void setDeliveryDelay(long deliveryDelay) throws JMSException {
        checkClosed();
        enforceDelayedDeliverySupport();
        producer.setDeliveryDelay(deliveryDelay);
    }

    @Override
    public long getDeliveryDelay() throws JMSException {
        checkClosed();
        enforceDelayedDeliverySupport();
        return producer.getDeliveryDelay();
    }

    @Override
    public Destination getDestination() throws JMSException {
        checkClosed();
        return destination;
    }

    @Override
    public Queue getQueue() throws JMSException {
        return (Queue) getDestination();
    }

    @Override
    public Topic getTopic() throws JMSException {
        return (Topic) getDestination();
    }

    @Override
    public void publish(Message message) throws JMSException {
        checkClosed();
        producer.send(message);
    }

    @Override
    public void publish(Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        checkClosed();
        producer.send(message, deliveryMode, priority, timeToLive);
    }

    @Override
    public void publish(Topic topic, Message message) throws JMSException {
        checkClosed();
        producer.send(topic, message);
    }

    @Override
    public void publish(Topic topic, Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        checkClosed();
        producer.send(topic, message, deliveryMode, priority, timeToLive);
    }

    @Override
    public void send(Message message) throws JMSException {
        checkClosed();
        producer.send(message);
    }

    @Override
    public void send(Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        checkClosed();
        producer.send(message, deliveryMode, priority, timeToLive);
    }

    @Override
    public void send(Queue queue, Message message) throws JMSException {
        checkClosed();
        producer.send(queue, message);
    }

    @Override
    public void send(Queue queue, Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        checkClosed();
        producer.send(queue, message, deliveryMode, priority, timeToLive);
    }

    @Override
    public void send(Message message, CompletionListener completionListener) throws JMSException {
        checkClosed();
        enforceCompletionListenerSupport();
        producer.send(message, completionListener);
    }

    @Override
    public void send(Message message, int deliveryMode, int priority, long timeToLive, CompletionListener completionListener) throws JMSException {
        checkClosed();
        enforceCompletionListenerSupport();
        producer.send(message, deliveryMode, priority, timeToLive, completionListener);
    }

    @Override
    public void send(Destination destination, Message message) throws JMSException {
        checkClosed();
        producer.send(destination, message);
    }

    @Override
    public void send(Destination destination, Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        checkClosed();
        producer.send(destination, message, deliveryMode, priority, timeToLive);
    }

    @Override
    public void send(Destination destination, Message message, CompletionListener completionListener) throws JMSException {
        checkClosed();
        enforceCompletionListenerSupport();
        producer.send(destination, message, completionListener);
    }

    @Override
    public void send(Destination destination, Message message, int deliveryMode, int priority, long timeToLive, CompletionListener completionListener) throws JMSException {
        checkClosed();
        enforceCompletionListenerSupport();
        producer.send(destination, message, deliveryMode, priority, timeToLive, completionListener);
    }

    /**
     * Gets the state of the producer closed flag.
     *
     * @return <code>true</code> if the producer has been closed.
     */
    protected boolean isClosed() {
        return closed;
    }

    /**
     * Checks for closure of this producer wrapper and throws if true.
     *
     * @throws IllegalStateException if the producer is closed.
     */
    protected void checkClosed() throws IllegalStateException {
        if (isClosed()) {
            throw new IllegalStateException("The producer has already been closed");
        }
    }

    private boolean checkIfProducerSilentlyClosed() {
        boolean seemsClosed = false;

        try {
            // Try and test the JMS resource to validate if it is still active.
            producer.getDestination();
        } catch (IllegalStateException jmsISE) {
            // The provider producer appears to be closed due something internal to the provider
            seemsClosed = true;
        } catch (Exception ambiguous) {
            // Not clear that the resource is closed so we don't assume it is.
        }

        return seemsClosed;
    }
}
