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

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;

import org.messaginghub.pooled.jms.internal.JmsPoolSessionProxy;
import org.messaginghub.pooled.jms.util.JMSExceptionSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.jms.BytesMessage;
import jakarta.jms.Destination;
import jakarta.jms.IllegalStateException;
import jakarta.jms.JMSException;
import jakarta.jms.MapMessage;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageListener;
import jakarta.jms.MessageProducer;
import jakarta.jms.ObjectMessage;
import jakarta.jms.Queue;
import jakarta.jms.QueueBrowser;
import jakarta.jms.QueueReceiver;
import jakarta.jms.QueueSender;
import jakarta.jms.QueueSession;
import jakarta.jms.Session;
import jakarta.jms.StreamMessage;
import jakarta.jms.TemporaryQueue;
import jakarta.jms.TemporaryTopic;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;
import jakarta.jms.TopicPublisher;
import jakarta.jms.TopicSession;
import jakarta.jms.TopicSubscriber;

/**
 * Session that has been taken from a pool of sessions maintained by a pooled JMS Connection.
 * <p>
 * The application code has full ownership of the pooled session instance until it closes its
 * wrapper object at which time the session is returned to the connection's pool for use by a
 * new call to create a session.
 */
public class JmsPoolSession implements Session, TopicSession, QueueSession, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final AtomicIntegerFieldUpdater<JmsPoolSession> CLOSED_UPDATER =
        AtomicIntegerFieldUpdater.newUpdater(JmsPoolSession.class, "closed");

    private final Map<AutoCloseable, AutoCloseable> resources = new ConcurrentHashMap<>();
    private final Map<JmsPoolSessionEventListener, JmsPoolSessionEventListener> sessionEventListeners = new ConcurrentHashMap<>();
    private final JmsPoolSessionProxy session;
    private final boolean transactional;

    private volatile int closed;

    JmsPoolSession(JmsPoolSessionProxy session, boolean transactional) {
        this.session = session;
        this.transactional = transactional;
    }

    @Override
    public void close() throws JMSException {
        internalClose(false);
    }

    void internalClose(boolean forceInvalidate) throws JMSException {
        if (CLOSED_UPDATER.compareAndSet(this, 0, 1)) {
            final boolean invalidate = cleanupSession() || forceInvalidate;

            if (invalidate) {
                // lets close the session and not put the session back into the pool
                // instead invalidate it so the pool can create a new one on demand.
                try {
                    session.destroy();
                } catch (Exception e) {
                    LOG.trace("Ignoring exception on invalidateSession as discarding session: " + e.getMessage(), e);
                }
            } else {
                // Release the session which allows it to return to the pool of sessions
                // for use by another caller to the connection create session APIs
                try {
                    session.close();
                } catch (Exception e) {
                    jakarta.jms.IllegalStateException illegalStateException = new jakarta.jms.IllegalStateException(e.toString());
                    illegalStateException.initCause(e);
                    throw illegalStateException;
                }
            }
        }
    }

    private boolean cleanupSession() {
        final AtomicReference<Exception> cleanupError = new AtomicReference<>();

        try {
            session.setMessageListener(null);
        } catch (JMSException e) {
            cleanupError.compareAndSet(null, e);
        }

        resources.keySet().forEach(resource -> {
            try {
                resource.close();
            } catch (Exception e) {
                LOG.trace("Caught exception trying close a session resource:{}, will invalidate. " + e.getMessage(), resource, e);
                cleanupError.compareAndSet(null, e);
            }
        });

        if (isRollbackOnClose()) {
            try {
                session.rollback();
            } catch (JMSException e) {
                LOG.warn("Caught exception trying rollback() when putting session back into the pool, will invalidate. " + e, e);
                cleanupError.compareAndSet(null, e);
            }
        }

        sessionEventListeners.keySet().forEach(listener -> {
            try {
                listener.onSessionClosed(this);
            } catch (Exception e) {
                cleanupError.compareAndSet(null, e);
            }
        });

        if (cleanupError.get() != null) {
            LOG.warn("Caught exception trying close() when putting session back into the pool, will invalidate. " + cleanupError, cleanupError);
        }

        return cleanupError.get() != null;
    }

    //----- Destination factory methods --------------------------------------//

    @Override
    public TemporaryQueue createTemporaryQueue() throws JMSException {
        final TemporaryQueue result = safeGetSessionProxy().createTemporaryQueue();

        // Notify all of the listeners of the created temporary Queue.
        sessionEventListeners.keySet().forEach(listener -> {
            listener.onTemporaryQueueCreate(result);
        });

        return result;
    }

    @Override
    public TemporaryTopic createTemporaryTopic() throws JMSException {
        final TemporaryTopic result = safeGetSessionProxy().createTemporaryTopic();

        // Notify all of the listeners of the created temporary Topic.
        sessionEventListeners.keySet().forEach(listener -> {
            listener.onTemporaryTopicCreate(result);
        });

        return result;
    }

    @Override
    public Queue createQueue(String name) throws JMSException {
        return safeGetSessionProxy().createQueue(name);
    }

    @Override
    public Topic createTopic(String name) throws JMSException {
        return safeGetSessionProxy().createTopic(name);
    }

    //----- Message factory methods ------------------------------------------//

    @Override
    public BytesMessage createBytesMessage() throws JMSException {
        return safeGetSessionProxy().createBytesMessage();
    }

    @Override
    public MapMessage createMapMessage() throws JMSException {
        return safeGetSessionProxy().createMapMessage();
    }

    @Override
    public Message createMessage() throws JMSException {
        return safeGetSessionProxy().createMessage();
    }

    @Override
    public ObjectMessage createObjectMessage() throws JMSException {
        return safeGetSessionProxy().createObjectMessage();
    }

    @Override
    public ObjectMessage createObjectMessage(Serializable serializable) throws JMSException {
        return safeGetSessionProxy().createObjectMessage(serializable);
    }

    @Override
    public StreamMessage createStreamMessage() throws JMSException {
        return safeGetSessionProxy().createStreamMessage();
    }

    @Override
    public TextMessage createTextMessage() throws JMSException {
        return safeGetSessionProxy().createTextMessage();
    }

    @Override
    public TextMessage createTextMessage(String s) throws JMSException {
        return safeGetSessionProxy().createTextMessage(s);
    }

    //----- Session management APIs ------------------------------------------//

    @Override
    public void unsubscribe(String subscriptionName) throws JMSException {
        safeGetSessionProxy().unsubscribe(subscriptionName);
    }

    @Override
    public int getAcknowledgeMode() throws JMSException {
        return safeGetSessionProxy().getAcknowledgeMode();
    }

    @Override
    public boolean getTransacted() throws JMSException {
        return safeGetSessionProxy().getTransacted();
    }

    @Override
    public void recover() throws JMSException {
        safeGetSessionProxy().recover();
    }

    @Override
    public void commit() throws JMSException {
        safeGetSessionProxy().commit();
    }

    @Override
    public void rollback() throws JMSException {
        safeGetSessionProxy().rollback();
    }

    //----- Java EE Session run entry point ----------------------------------//

    @Override
    public MessageListener getMessageListener() throws JMSException {
        return safeGetSessionProxy().getMessageListener();
    }

    @Override
    public void setMessageListener(MessageListener messageListener) throws JMSException {
        safeGetSessionProxy().setMessageListener(messageListener);
    }

    @Override
    public void run() {
        final JmsPoolSessionProxy session;

        try {
            session = safeGetSessionProxy();
        } catch (JMSException e) {
            throw JMSExceptionSupport.createRuntimeException(e);
        }

        if (session != null) {
            session.run();
        }
    }

    //----- Consumer related methods -----------------------------------------//

    @Override
    public QueueBrowser createBrowser(Queue queue) throws JMSException {
        return addCloseable(new JmsPoolQueueBrowser(safeGetSessionProxy().createBrowser(queue), this::onQueueBrowserClose));
    }

    @Override
    public QueueBrowser createBrowser(Queue queue, String selector) throws JMSException {
        return addCloseable(new JmsPoolQueueBrowser(safeGetSessionProxy().createBrowser(queue, selector), this::onQueueBrowserClose));
    }

    @Override
    public MessageConsumer createConsumer(Destination destination) throws JMSException {
        return addCloseable(new JmsPoolMessageConsumer(safeGetSessionProxy().createConsumer(destination), this::onConsumerClose));
    }

    @Override
    public MessageConsumer createConsumer(Destination destination, String selector) throws JMSException {
        return addCloseable(new JmsPoolMessageConsumer(safeGetSessionProxy().createConsumer(destination, selector), this::onConsumerClose));
    }

    @Override
    public MessageConsumer createConsumer(Destination destination, String selector, boolean noLocal) throws JMSException {
        return addCloseable(new JmsPoolMessageConsumer(safeGetSessionProxy().createConsumer(destination, selector, noLocal), this::onConsumerClose));
    }

    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String selector) throws JMSException {
        return addCloseable(new JmsPoolTopicSubscriber(safeGetSessionProxy().createDurableSubscriber(topic, selector), this::onConsumerClose));
    }

    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String name, String selector, boolean noLocal) throws JMSException {
        return addCloseable(new JmsPoolTopicSubscriber(safeGetSessionProxy().createDurableSubscriber(topic, name, selector, noLocal), this::onConsumerClose));
    }

    @Override
    public TopicSubscriber createSubscriber(Topic topic) throws JMSException {
        return addCloseable(new JmsPoolTopicSubscriber(safeGetSessionProxy().createSubscriber(topic), this::onConsumerClose));
    }

    @Override
    public TopicSubscriber createSubscriber(Topic topic, String selector, boolean local) throws JMSException {
        return addCloseable(new JmsPoolTopicSubscriber(safeGetSessionProxy().createSubscriber(topic, selector, local), this::onConsumerClose));
    }

    @Override
    public QueueReceiver createReceiver(Queue queue) throws JMSException {
        return addCloseable(new JmsPoolQueueReceiver(safeGetSessionProxy().createReceiver(queue), this::onConsumerClose));
    }

    @Override
    public QueueReceiver createReceiver(Queue queue, String selector) throws JMSException {
        return addCloseable(new JmsPoolQueueReceiver(safeGetSessionProxy().createReceiver(queue, selector), this::onConsumerClose));
    }

    //----- JMS 2.0 Subscriber creation API ----------------------------------//

    @Override
    public MessageConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName) throws JMSException {
        // TODO : Enforce version of JMS supports this feature
        return addCloseable(new JmsPoolMessageConsumer(safeGetSessionProxy().createSharedConsumer(topic, sharedSubscriptionName), this::onConsumerClose));
    }

    @Override
    public MessageConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName, String messageSelector) throws JMSException {
        // TODO : Enforce version of JMS supports this feature
        return addCloseable(new JmsPoolMessageConsumer(safeGetSessionProxy().createSharedConsumer(topic, sharedSubscriptionName, messageSelector), this::onConsumerClose));
    }

    @Override
    public MessageConsumer createDurableConsumer(Topic topic, String name) throws JMSException {
        // TODO : Enforce version of JMS supports this feature
        return addCloseable(new JmsPoolMessageConsumer(safeGetSessionProxy().createDurableConsumer(topic, name), this::onConsumerClose));
    }

    @Override
    public MessageConsumer createDurableConsumer(Topic topic, String name, String messageSelector, boolean noLocal) throws JMSException {
        // TODO : Enforce version of JMS supports this feature
        return addCloseable(new JmsPoolMessageConsumer(safeGetSessionProxy().createDurableConsumer(topic, name, messageSelector, noLocal), this::onConsumerClose));
    }

    @Override
    public MessageConsumer createSharedDurableConsumer(Topic topic, String name) throws JMSException {
        // TODO : Enforce version of JMS supports this feature
        return addCloseable(new JmsPoolMessageConsumer(safeGetSessionProxy().createSharedDurableConsumer(topic, name), this::onConsumerClose));
    }

    @Override
    public MessageConsumer createSharedDurableConsumer(Topic topic, String name, String messageSelector) throws JMSException {
        // TODO : Enforce version of JMS supports this feature
        return addCloseable(new JmsPoolMessageConsumer(safeGetSessionProxy().createSharedDurableConsumer(topic, name, messageSelector), this::onConsumerClose));
    }

    //----- Producer related methods -----------------------------------------//

    @Override
    public MessageProducer createProducer(Destination destination) throws JMSException {
        return addCloseable(new JmsPoolMessageProducer(safeGetSessionProxy().createProducer(destination), destination, this::onProducerClosed));
    }

    @Override
    public QueueSender createSender(Queue queue) throws JMSException {
        return addCloseable(new JmsPoolQueueSender(safeGetSessionProxy().createSender(queue), queue, this::onProducerClosed));
    }

    @Override
    public TopicPublisher createPublisher(Topic topic) throws JMSException {
        return addCloseable(new JmsPoolTopicPublisher(safeGetSessionProxy().createPublisher(topic), topic, this::onProducerClosed));
    }

    //----- Session configuration methods ------------------------------------//

    /**
     * Adds a listener to the pooled session wrapper for some specific life-cycle events.
     *
     * @param listener
     * 	The new event listener to add to the set assigned to this wrapper instance.
     *
     * @throws JMSException if an error occurs while attempting to add the event listener.
     */
    public void addSessionEventListener(JmsPoolSessionEventListener listener) throws JMSException {
        checkClosed();
        sessionEventListeners.put(listener, listener);
    }

    /**
     * Provides a means of accessing the underlying JMS {@link Session} that this pooled session
     * wrapper is managing. This is mainly a test point and should not be used by application logic.
     *
     * @return the underling JMS {@link Session} that this object is wrapping.
     *
     * @throws JMSException if an error occurs while attempting to access the session.
     */
    Session getProviderSession() throws JMSException {
        return safeGetSessionProxy().getProviderSession();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " { " + session + " }";
    }

    //----- Consumer callback methods ----------------------------------------//

    /**
     * Callback invoked when the consumer is closed.
     * <p>
     * This is used to keep track of an explicit closed consumer created by this
     * session so that the internal tracking data structures can be cleaned up.
     *
     * @param consumer
     * 		the consumer which is being closed.
     */
    protected void onConsumerClose(JmsPoolMessageConsumer consumer) {
        resources.remove(consumer);
    }

    /**
     * Callback invoked when the consumer is closed.
     * <p>
     * This is used to keep track of an explicit closed browser created by this
     * session so that the internal tracking data structures can be cleaned up.
     *
     * @param browser
     * 		the browser which is being closed.
     */
    protected void onQueueBrowserClose(JmsPoolQueueBrowser browser) {
        resources.remove(browser);
    }

    /**
     * Callback invoked when the producer is closed.
     * <p>
     * This is used to keep track of an explicit closed producer created by this
     * session so that the internal tracking data structures can be cleaned up.
     *
     * @param producer
     * 		the producer which is being closed.
     */
    protected void onProducerClosed(JmsPoolMessageProducer producer) {
        resources.remove(producer);
    }

    //----- Internal support methods -----------------------------------------//

    private void checkClosed() throws IllegalStateException {
        if (closed != 0) {
            throw new IllegalStateException("Session has already been closed");
        }
    }

    private <T extends AutoCloseable> T addCloseable(T closeable) {
        resources.put(closeable, closeable);
        return closeable;
    }

    protected boolean isTransactional() {
        return transactional;
    }

    protected boolean isRollbackOnClose() {
        return transactional;
    }

    protected JmsPoolSessionProxy safeGetSessionProxy() throws JMSException {
        checkClosed();
        return this.session;
    }
}
