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

import javax.transaction.xa.XAResource;

import org.apache.commons.pool2.KeyedObjectPool;
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
import jakarta.jms.XASession;

/**
 * Session that has been taken from a pool of sessions maintained by a pooled JMS Connection.
 * <p>
 * The application code has full ownership of the pooled session instance until it closes its
 * wrapper object at which time the session is returned to the connection's pool for use by a
 * new call to create a session.
 */
public class JmsPoolSession implements Session, TopicSession, QueueSession, XASession, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final AtomicIntegerFieldUpdater<JmsPoolSession> CLOSED_UPDATER =
        AtomicIntegerFieldUpdater.newUpdater(JmsPoolSession.class, "closed");

    private final JmsPoolSessionKey key;
    private final KeyedObjectPool<JmsPoolSessionKey, JmsPoolSharedSession> sessionPool;
    private final Map<AutoCloseable, AutoCloseable> resources = new ConcurrentHashMap<>();
    private final Map<JmsPoolSessionEventListener, JmsPoolSessionEventListener> sessionEventListeners = new ConcurrentHashMap<>();

    private volatile int closed;

    private JmsPoolSharedSession sessionHolder;
    private boolean transactional;
    private boolean ignoreClose;
    private boolean isXa;

    JmsPoolSession(JmsPoolSessionKey key, JmsPoolSharedSession sessionHolder, KeyedObjectPool<JmsPoolSessionKey, JmsPoolSharedSession> sessionPool, boolean transactional) {
        this.key = key;
        this.sessionHolder = sessionHolder;
        this.sessionPool = sessionPool;
        this.transactional = transactional;
    }

    @Override
    public void close() throws JMSException {
        if (!ignoreClose) {
            internalClose(false);
        }
    }

    void internalClose(boolean forceInvalidate) throws JMSException {
        if (CLOSED_UPDATER.compareAndSet(this, 0, 1)) {
            final boolean invalidate = cleanupSession() || forceInvalidate;

            if (invalidate) {
                // lets close the session and not put the session back into the pool
                // instead invalidate it so the pool can create a new one on demand.
                if (sessionHolder != null) {
                    try {
                        sessionHolder.close();
                    } catch (JMSException e1) {
                        LOG.trace("Ignoring exception on close as discarding session: " + e1, e1);
                    }
                }

                try {
                    sessionPool.invalidateObject(key, sessionHolder);
                } catch (Exception e) {
                    LOG.trace("Ignoring exception on invalidateObject as discarding session: " + e, e);
                }
            } else {
                try {
                    sessionPool.returnObject(key, sessionHolder);
                } catch (Exception e) {
                    jakarta.jms.IllegalStateException illegalStateException = new jakarta.jms.IllegalStateException(e.toString());
                    illegalStateException.initCause(e);
                    throw illegalStateException;
                }
            }

            sessionHolder = null;
        }
    }

    private boolean cleanupSession() {
        final AtomicReference<Exception> cleanupError = new AtomicReference<>();

        try {
            getInternalSession().setMessageListener(null);
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

        if (transactional && !isXa) {
            try {
                getInternalSession().rollback();
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
        final TemporaryQueue result = getInternalSession().createTemporaryQueue();

        // Notify all of the listeners of the created temporary Queue.
        sessionEventListeners.keySet().forEach(listener -> {
            listener.onTemporaryQueueCreate(result);
        });

        return result;
    }

    @Override
    public TemporaryTopic createTemporaryTopic() throws JMSException {
        final TemporaryTopic result = getInternalSession().createTemporaryTopic();

        // Notify all of the listeners of the created temporary Topic.
        sessionEventListeners.keySet().forEach(listener -> {
            listener.onTemporaryTopicCreate(result);
        });

        return result;
    }

    @Override
    public Queue createQueue(String s) throws JMSException {
        return getInternalSession().createQueue(s);
    }

    @Override
    public Topic createTopic(String s) throws JMSException {
        return getInternalSession().createTopic(s);
    }

    //----- Message factory methods ------------------------------------------//

    @Override
    public BytesMessage createBytesMessage() throws JMSException {
        return getInternalSession().createBytesMessage();
    }

    @Override
    public MapMessage createMapMessage() throws JMSException {
        return getInternalSession().createMapMessage();
    }

    @Override
    public Message createMessage() throws JMSException {
        return getInternalSession().createMessage();
    }

    @Override
    public ObjectMessage createObjectMessage() throws JMSException {
        return getInternalSession().createObjectMessage();
    }

    @Override
    public ObjectMessage createObjectMessage(Serializable serializable) throws JMSException {
        return getInternalSession().createObjectMessage(serializable);
    }

    @Override
    public StreamMessage createStreamMessage() throws JMSException {
        return getInternalSession().createStreamMessage();
    }

    @Override
    public TextMessage createTextMessage() throws JMSException {
        return getInternalSession().createTextMessage();
    }

    @Override
    public TextMessage createTextMessage(String s) throws JMSException {
        return getInternalSession().createTextMessage(s);
    }

    //----- Session management APIs ------------------------------------------//

    @Override
    public void unsubscribe(String s) throws JMSException {
        getInternalSession().unsubscribe(s);
    }

    @Override
    public int getAcknowledgeMode() throws JMSException {
        return getInternalSession().getAcknowledgeMode();
    }

    @Override
    public boolean getTransacted() throws JMSException {
        return getInternalSession().getTransacted();
    }

    @Override
    public void recover() throws JMSException {
        getInternalSession().recover();
    }

    @Override
    public void commit() throws JMSException {
        getInternalSession().commit();
    }

    @Override
    public void rollback() throws JMSException {
        getInternalSession().rollback();
    }

    @Override
    public XAResource getXAResource() {
        final JmsPoolSharedSession session;
        try {
            session = safeGetSessionHolder();
        } catch (JMSException e) {
            throw JMSExceptionSupport.createRuntimeException(e);
        }

        if (session.getSession() instanceof XASession) {
            return ((XASession) session.getSession()).getXAResource();
        }

        return null;
    }

    @Override
    public Session getSession() {
        return this;
    }

    //----- Java EE Session run entry point ----------------------------------//

    @Override
    public MessageListener getMessageListener() throws JMSException {
        return getInternalSession().getMessageListener();
    }

    @Override
    public void setMessageListener(MessageListener messageListener) throws JMSException {
        getInternalSession().setMessageListener(messageListener);
    }

    @Override
    public void run() {
        final JmsPoolSharedSession session;
        try {
            session = safeGetSessionHolder();
        } catch (JMSException e) {
            throw JMSExceptionSupport.createRuntimeException(e);
        }

        if (session != null) {
            session.getSession().run();
        }
    }

    //----- Consumer related methods -----------------------------------------//

    @Override
    public QueueBrowser createBrowser(Queue queue) throws JMSException {
        return addQueueBrowser(getInternalSession().createBrowser(queue));
    }

    @Override
    public QueueBrowser createBrowser(Queue queue, String selector) throws JMSException {
        return addQueueBrowser(getInternalSession().createBrowser(queue, selector));
    }

    @Override
    public MessageConsumer createConsumer(Destination destination) throws JMSException {
        return addConsumer(getInternalSession().createConsumer(destination));
    }

    @Override
    public MessageConsumer createConsumer(Destination destination, String selector) throws JMSException {
        return addConsumer(getInternalSession().createConsumer(destination, selector));
    }

    @Override
    public MessageConsumer createConsumer(Destination destination, String selector, boolean noLocal) throws JMSException {
        return addConsumer(getInternalSession().createConsumer(destination, selector, noLocal));
    }

    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String selector) throws JMSException {
        return addTopicSubscriber(getInternalSession().createDurableSubscriber(topic, selector));
    }

    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String name, String selector, boolean noLocal) throws JMSException {
        return addTopicSubscriber(getInternalSession().createDurableSubscriber(topic, name, selector, noLocal));
    }

    @Override
    public TopicSubscriber createSubscriber(Topic topic) throws JMSException {
        return addTopicSubscriber(((TopicSession) getInternalSession()).createSubscriber(topic));
    }

    @Override
    public TopicSubscriber createSubscriber(Topic topic, String selector, boolean local) throws JMSException {
        return addTopicSubscriber(((TopicSession) getInternalSession()).createSubscriber(topic, selector, local));
    }

    @Override
    public QueueReceiver createReceiver(Queue queue) throws JMSException {
        return addQueueReceiver(((QueueSession) getInternalSession()).createReceiver(queue));
    }

    @Override
    public QueueReceiver createReceiver(Queue queue, String selector) throws JMSException {
        return addQueueReceiver(((QueueSession) getInternalSession()).createReceiver(queue, selector));
    }

    //----- JMS 2.0 Subscriber creation API ----------------------------------//

    @Override
    public MessageConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName) throws JMSException {
        JmsPoolSharedSession state = safeGetSessionHolder();
        state.getConnection().checkClientJMSVersionSupport(2, 0);
        return addConsumer(state.getSession().createSharedConsumer(topic, sharedSubscriptionName));
    }

    @Override
    public MessageConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName, String messageSelector) throws JMSException {
        JmsPoolSharedSession state = safeGetSessionHolder();
        state.getConnection().checkClientJMSVersionSupport(2, 0);
        return addConsumer(state.getSession().createSharedConsumer(topic, sharedSubscriptionName, messageSelector));
    }

    @Override
    public MessageConsumer createDurableConsumer(Topic topic, String name) throws JMSException {
        JmsPoolSharedSession state = safeGetSessionHolder();
        state.getConnection().checkClientJMSVersionSupport(2, 0);
        return addConsumer(state.getSession().createDurableConsumer(topic, name));
    }

    @Override
    public MessageConsumer createDurableConsumer(Topic topic, String name, String messageSelector, boolean noLocal) throws JMSException {
        JmsPoolSharedSession state = safeGetSessionHolder();
        state.getConnection().checkClientJMSVersionSupport(2, 0);
        return addConsumer(state.getSession().createDurableConsumer(topic, name, messageSelector, noLocal));
    }

    @Override
    public MessageConsumer createSharedDurableConsumer(Topic topic, String name) throws JMSException {
        JmsPoolSharedSession state = safeGetSessionHolder();
        state.getConnection().checkClientJMSVersionSupport(2, 0);
        return addConsumer(state.getSession().createSharedDurableConsumer(topic, name));
    }

    @Override
    public MessageConsumer createSharedDurableConsumer(Topic topic, String name, String messageSelector) throws JMSException {
        JmsPoolSharedSession state = safeGetSessionHolder();
        state.getConnection().checkClientJMSVersionSupport(2, 0);
        return addConsumer(state.getSession().createSharedDurableConsumer(topic, name, messageSelector));
    }

    //----- Producer related methods -----------------------------------------//

    @Override
    public MessageProducer createProducer(Destination destination) throws JMSException {
        final JmsPoolMessageProducer result = safeGetSessionHolder().getOrCreateProducer(this, destination);
        resources.put(result, result);
        return result;
    }

    @Override
    public QueueSender createSender(Queue queue) throws JMSException {
        final JmsPoolQueueSender result = safeGetSessionHolder().getOrCreateSender(this, queue);
        resources.put(result, result);
        return result;
    }

    @Override
    public TopicPublisher createPublisher(Topic topic) throws JMSException {
        final JmsPoolTopicPublisher result = safeGetSessionHolder().getOrCreatePublisher(this, topic);
        resources.put(result, result);
        return result;
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
    public Session getInternalSession() throws JMSException {
        return safeGetSessionHolder().getSession();
    }

    void setIsXa(boolean isXa) {
        this.isXa = isXa;
    }

    boolean isIgnoreClose() {
        return ignoreClose;
    }

    void setIgnoreClose(boolean ignoreClose) {
        this.ignoreClose = ignoreClose;
    }

    @Override
    public String toString() {
        try {
            return getClass().getSimpleName() + " { " + safeGetSessionHolder() + " }";
        } catch (JMSException e) {
            return getClass().getSimpleName() + " { " + null + " }";
        }
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
     * @param force
     * 		should the producer be closed regardless of other configuration
     *
     * @throws JMSException if an error occurs while closing the provider MessageProducer.
     */
    protected void onMessageProducerClosed(JmsPoolMessageProducer producer, boolean force) throws JMSException {
        resources.remove(producer);
        safeGetSessionHolder().onJmsPoolProducerClosed(producer, force);
    }

    //----- Internal support methods -----------------------------------------//

    protected void checkClientJMSVersionSupport(int major, int minor) throws JMSException {
        safeGetSessionHolder().getConnection().checkClientJMSVersionSupport(major, minor);
    }

    protected boolean isJMSVersionSupported(int major, int minor) throws JMSException {
        return safeGetSessionHolder().getConnection().isJMSVersionSupported(major, minor);
    }

    private void checkClosed() throws IllegalStateException {
        if (closed != 0) {
            throw new IllegalStateException("Session has already been closed");
        }
    }

    private QueueBrowser addQueueBrowser(QueueBrowser browser) {
        browser = new JmsPoolQueueBrowser(this, browser);
        resources.put(browser, browser);
        return browser;
    }

    private MessageConsumer addConsumer(MessageConsumer consumer) {
        consumer = new JmsPoolMessageConsumer(this, consumer);
        resources.put(consumer, consumer);
        return consumer;
    }

    private TopicSubscriber addTopicSubscriber(TopicSubscriber subscriber) {
        subscriber = new JmsPoolTopicSubscriber(this, subscriber);
        resources.put(subscriber, subscriber);
        return subscriber;
    }

    private QueueReceiver addQueueReceiver(QueueReceiver receiver) {
        receiver = new JmsPoolQueueReceiver(this, receiver);
        resources.put(receiver, receiver);
        return receiver;
    }

    private JmsPoolSharedSession safeGetSessionHolder() throws JMSException {
        final JmsPoolSharedSession sessionHolder = this.sessionHolder;

        if (sessionHolder == null) {
            throw new IllegalStateException("The session has already been closed");
        }

        return sessionHolder;
    }
}
