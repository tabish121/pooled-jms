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

import org.messaginghub.pooled.jms.internal.JmsMessageProducerProxy;

import jakarta.jms.CompletionListener;
import jakarta.jms.Destination;
import jakarta.jms.IllegalStateException;
import jakarta.jms.InvalidDestinationException;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageProducer;

/**
 * A pooled {@link MessageProducer}
 */
public class JmsPoolMessageProducer implements MessageProducer, AutoCloseable {

    private static final AtomicIntegerFieldUpdater<JmsPoolMessageProducer> CLOSED_UPDATER =
        AtomicIntegerFieldUpdater.newUpdater(JmsPoolMessageProducer.class, "closed");

    private final JmsMessageProducerProxy messageProducer;
    private final Consumer<JmsPoolMessageProducer> onClose;
    private final Destination destination;

    private final boolean anonymousProducer;

    private volatile int closed;
    private int deliveryMode;
    private boolean disableMessageID;
    private boolean disableMessageTimestamp;
    private int priority;
    private long timeToLive;
    private long deliveryDelay;

    JmsPoolMessageProducer(JmsMessageProducerProxy messageProducer, Destination destination, Consumer<JmsPoolMessageProducer> onClose) throws JMSException {
        this.messageProducer = messageProducer;
        this.destination = destination;
        this.anonymousProducer = destination == null;
        this.onClose = onClose;

        try {
            this.deliveryMode = messageProducer.getDeliveryMode();
            this.disableMessageID = messageProducer.getDisableMessageID();
            this.disableMessageTimestamp = messageProducer.getDisableMessageTimestamp();
            this.priority = messageProducer.getPriority();
            this.timeToLive = messageProducer.getTimeToLive();

            if (messageProducer.isDelayedDeliverySupported()) {
                this.deliveryDelay = messageProducer.getDeliveryDelay();
            }
        } catch (Exception e) {
            try {
                messageProducer.destroy();
            } catch (Exception ignore) {
                // Ignored in place of original exception
            }

            throw e;
        }
    }

    @Override
    public void close() throws JMSException {
        doClose(false);
    }

    //----- JMS 1.0 Send Methods ---------------------------------------------//

    @Override
    public void send(Message message) throws JMSException {
        send(message, deliveryMode, priority, timeToLive);
    }

    @Override
    public void send(Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        checkClosed();

        if (anonymousProducer) {
            throw new UnsupportedOperationException("Using this method is not supported on producers created without an explicit Destination");
        }

        doSendMessage(destination, message, deliveryMode, priority, timeToLive,
                      deliveryDelay, disableMessageID, disableMessageTimestamp, null);
    }

    @Override
    public void send(Destination destination, Message message) throws JMSException {
        sendMessage(destination, message, deliveryMode, priority, timeToLive, deliveryDelay, disableMessageID, disableMessageTimestamp);
    }

    @Override
    public void send(Destination destination, Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        sendMessage(destination, message, deliveryMode, priority, timeToLive, deliveryDelay, disableMessageID, disableMessageTimestamp);
    }

    //----- JMS 2.0 Send methods ---------------------------------------------//

    @Override
    public void send(Message message, CompletionListener listener) throws JMSException {
        send(message, deliveryMode, priority, timeToLive, listener);
    }

    @Override
    public void send(Message message, int deliveryMode, int priority, long timeToLive, CompletionListener listener) throws JMSException {
        checkClosed();

        if (anonymousProducer) {
            throw new UnsupportedOperationException("Using this method is not supported on producers created without an explicit Destination");
        }

        if (listener == null) {
            throw new IllegalArgumentException("CompletionListener cannot be null");
        }

        doSendMessage(destination, message, deliveryMode, priority, timeToLive,
                      deliveryDelay, disableMessageID, disableMessageTimestamp, listener);
    }

    @Override
    public void send(Destination destination, Message message, CompletionListener listener) throws JMSException {
        sendMessage(destination, message, deliveryMode, priority, timeToLive, deliveryDelay, disableMessageID, disableMessageTimestamp, listener);
    }

    @Override
    public void send(Destination destination, Message message, int deliveryMode, int priority, long timeToLive, CompletionListener listener) throws JMSException {
        sendMessage(destination, message, deliveryMode, priority, timeToLive, deliveryDelay, disableMessageID, disableMessageTimestamp, listener);
    }

    //----- Internal and send implementations --------------------------------//

    void sendMessage(Destination destination, Message message, int deliveryMode, int priority, long timeToLive,
                     long deliveryDelay, boolean disableMessageID, boolean disableMessageTimestamp) throws JMSException {

        checkClosed();
        checkDestinationNotInvalid(destination);

        if (!anonymousProducer) {
            throw new UnsupportedOperationException(
                    "Using this method is not supported on producers created with an explicit Destination.");
        }

        doSendMessage(destination, message, deliveryMode, priority, timeToLive,
                      deliveryDelay, disableMessageID, disableMessageTimestamp, null);
    }

    void sendMessage(Destination destination, Message message, int deliveryMode, int priority, long timeToLive,
                     long deliveryDelay, boolean disableMessageID, boolean disableMessageTimestamp,
                     CompletionListener listener) throws JMSException {

        checkClosed();
        checkDestinationNotInvalid(destination);

        if (!anonymousProducer) {
            throw new UnsupportedOperationException("Using this method is not supported on producers created with an explicit Destination.");
        }

        if (listener == null) {
            throw new IllegalArgumentException("CompletionListener cannot be null");
        }

        doSendMessage(destination, message, deliveryMode, priority, timeToLive,
                      deliveryDelay, disableMessageID, disableMessageTimestamp, listener);
    }

    private void doSendMessage(Destination destination, Message message,
                               int deliveryMode, int priority, long timeToLive,
                               long deliveryDelay, boolean disableMessageID, boolean disableMessageTimestamp,
                               CompletionListener listener) throws JMSException {
        // Only one thread can use the producer at a time to allow for dynamic configuration
        // changes to match what's been configured here. It is tempting to optimize out the lock
        // if not actually applying changes here but another thread can call into this method
        // with different configuration which would mutate the producer while a send is in progress
        // which can cause the other send to not honor what was passed.
        synchronized (messageProducer) {
            long oldDeliveryDelay = 0;
            boolean oldDisableMessageID = false;
            boolean oldDisableMessageTimestamp = false;

            try {
                if (deliveryDelay != 0 && messageProducer.isDelayedDeliverySupported()) {
                    oldDeliveryDelay = messageProducer.getDeliveryDelay();
                    messageProducer.setDeliveryDelay(deliveryDelay);
                }
                if (disableMessageID) {
                    oldDisableMessageID = messageProducer.getDisableMessageID();
                    messageProducer.setDisableMessageID(disableMessageID);
                }
                if (disableMessageTimestamp) {
                    oldDisableMessageTimestamp = messageProducer.getDisableMessageTimestamp();
                    messageProducer.setDisableMessageTimestamp(disableMessageTimestamp);
                }
            } catch (IllegalStateException jmsISE) {
                // The JMS MessageProducer that backs this wrapper has indicated that it is
                // likely closed either individually or by the full connection having gone
                // away.  We will force close it in case only the producer was closed so that
                // future uses of the producer will result in a new one being opened if the
                // connection is still live.
                try {
                    doClose(true);
                } catch (Exception ex) {
                    // Ignore and throw original failure cause
                }

                throw jmsISE;
            }

            // For the non-shared MessageProducer that is also not an anonymous producer we
            // need to call the send method for an explicit MessageProducer otherwise we
            // would be violating the JMS specification in regards to send calls.
            //
            // In all other cases we create an anonymous producer so we call the send with
            // destination parameter version.
            boolean sendSuccess = false;

            try {
                if (!messageProducer.isAnonymousProducer()) {
                    if (listener == null) {
                        messageProducer.send(message, deliveryMode, priority, timeToLive);
                    } else {
                        messageProducer.send(message, deliveryMode, priority, timeToLive, listener);
                    }
                } else {
                    if (listener == null) {
                        messageProducer.send(destination, message, deliveryMode, priority, timeToLive);
                    } else {
                        messageProducer.send(destination, message, deliveryMode, priority, timeToLive, listener);
                    }
                }

                sendSuccess = true;
            } catch (IllegalStateException jmsISE) {
                // The JMS MessageProducer that backs this wrapper has indicated that it is
                // likely closed either individually or by the full connection having gone
                // away.  We will force close it in case only the producer was closed so that
                // future uses of the producer will result in a new one being opened if the
                // connection is still live.
                try {
                    doClose(true);
                } catch (Exception ex) {
                    // Ignore and throw original failure cause
                }

                throw jmsISE;
            } finally {
                if (!isClosed()) {
                    boolean forceClose = false;
                    JMSException jmsEx = null;

                    if (messageProducer.isDelayedDeliverySupported() && deliveryDelay != 0) {
                        try {
                            messageProducer.setDeliveryDelay(oldDeliveryDelay);
                        } catch (JMSException ex) {
                            forceClose = forceClose || ex instanceof IllegalStateException;
                            if (jmsEx == null) {
                                jmsEx = ex;
                            }
                        }
                    }
                    if (disableMessageID) {
                        try {
                            messageProducer.setDisableMessageID(oldDisableMessageID);
                        } catch (JMSException ex) {
                            forceClose = forceClose || ex instanceof IllegalStateException;
                            if (jmsEx == null) {
                                jmsEx = ex;
                            }
                        }
                    }
                    if (disableMessageTimestamp) {
                        try {
                            messageProducer.setDisableMessageTimestamp(oldDisableMessageTimestamp);
                        } catch (JMSException ex) {
                            forceClose = forceClose || ex instanceof IllegalStateException;
                            if (jmsEx == null) {
                                jmsEx = ex;
                            }
                        }
                    }

                    if (forceClose) {
                        try {
                            doClose(true);
                        } catch (Exception ex) {
                            // Ignore and throw original failure cause
                        }
                    }

                    if (sendSuccess && jmsEx != null) {
                        throw jmsEx;
                    }
                }
            }
        }
    }

    //----- MessageProducer configuration ------------------------------------//

    @Override
    public Destination getDestination() throws JMSException {
        checkClosed();
        return destination;
    }

    @Override
    public int getDeliveryMode() throws JMSException {
        checkClosed();
        return deliveryMode;
    }

    @Override
    public void setDeliveryMode(int deliveryMode) throws JMSException {
        checkClosed();
        this.deliveryMode = deliveryMode;
    }

    @Override
    public boolean getDisableMessageID() throws JMSException {
        checkClosed();
        return disableMessageID;
    }

    @Override
    public void setDisableMessageID(boolean disableMessageID) throws JMSException {
        checkClosed();
        this.disableMessageID = disableMessageID;
    }

    @Override
    public boolean getDisableMessageTimestamp() throws JMSException {
        checkClosed();
        return disableMessageTimestamp;
    }

    @Override
    public void setDisableMessageTimestamp(boolean disableMessageTimestamp) throws JMSException {
        checkClosed();
        this.disableMessageTimestamp = disableMessageTimestamp;
    }

    @Override
    public int getPriority() throws JMSException {
        checkClosed();
        return priority;
    }

    @Override
    public void setPriority(int priority) throws JMSException {
        checkClosed();
        this.priority = priority;
    }

    @Override
    public long getTimeToLive() throws JMSException {
        checkClosed();
        return timeToLive;
    }

    @Override
    public void setTimeToLive(long timeToLive) throws JMSException {
        checkClosed();
        this.timeToLive = timeToLive;
    }

    @Override
    public long getDeliveryDelay() throws JMSException {
        checkClosed();
        messageProducer.enforceDelayedDeliverySupport();
        return deliveryDelay;
    }

    @Override
    public void setDeliveryDelay(long deliveryDelay) throws JMSException {
        checkClosed();
        messageProducer.enforceDelayedDeliverySupport();
        this.deliveryDelay = deliveryDelay;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " { destination=" + destination + " }";
    }

    /**
     * Returns the {@link MessageProducer} instance that this wrapper delegates to.
     *
     * @return the JMS {@link MessageProducer} that this pooled wrapper delegates to.
     *
     * @throws JMSException if the producer has already been closed.
     */
    MessageProducer getProviderMessageProducer() throws JMSException {
        checkClosed();
        return messageProducer.getProviderMessageProducer();
    }

    //----- Internal Implementation ------------------------------------------//

    private void doClose(boolean force) throws JMSException {
        if (CLOSED_UPDATER.compareAndSet(this, 0, 1)) {
            try {
                // As with any JMS resource, a failure on close doesn't have a real
                // path to recovery. We pass along any exception here but consider this
                // as our attempt to close and if the provider did not close itself then
                // that is left as an issue in the provider instance.
                if (force) {
                    messageProducer.destroy();
                } else {
                    messageProducer.close();
                }
            } finally {
                onClose.accept(this);
            }
        }
    }

    private boolean isClosed() {
        return closed != 0;
    }

    private void checkClosed() throws IllegalStateException {
        if (isClosed()) {
            throw new IllegalStateException("This message producer has been closed.");
        }
    }

    private void checkDestinationNotInvalid(Destination destination) throws InvalidDestinationException {
        if (destination == null) {
            throw new InvalidDestinationException("Destination must not be null");
        }
    }
}
