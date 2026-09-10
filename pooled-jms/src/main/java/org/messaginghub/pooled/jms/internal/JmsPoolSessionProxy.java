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

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.messaginghub.pooled.jms.util.FixedReference;
import org.messaginghub.pooled.jms.util.JMSVersionSupport;
import org.messaginghub.pooled.jms.util.LRUCache;
import org.messaginghub.pooled.jms.util.ReferenceCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.jms.BytesMessage;
import jakarta.jms.Destination;
import jakarta.jms.IllegalStateException;
import jakarta.jms.IllegalStateRuntimeException;
import jakarta.jms.JMSException;
import jakarta.jms.MapMessage;
import jakarta.jms.Message;
import jakarta.jms.MessageListener;
import jakarta.jms.ObjectMessage;
import jakarta.jms.Queue;
import jakarta.jms.QueueSession;
import jakarta.jms.Session;
import jakarta.jms.StreamMessage;
import jakarta.jms.TemporaryQueue;
import jakarta.jms.TemporaryTopic;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;
import jakarta.jms.TopicSession;

/**
 * Used to store a pooled session instance and any resources that can be left open
 * and carried along with the pooled instance such as the anonymous producer used
 * for all MessageProducer instances created from this pooled session when enabled.
 * When the client code closes the session any resources that it created during the
 * time it was loaned to the client will be closed and cleared. If no errors occur
 * during the session cleanup it will be placed back into the pool of sessions for
 * use when a new call to create a session is made.
 */
public class JmsPoolSessionProxy implements Session, TopicSession, QueueSession {

    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final AtomicIntegerFieldUpdater<JmsPoolSessionProxy> CLOSED_UPDATER =
        AtomicIntegerFieldUpdater.newUpdater(JmsPoolSessionProxy.class, "closed");

    private final JmsPoolConnectionConfiguration configuration;
    private final Session session;
    private final GenericKeyedObjectPool<JmsPoolSessionKey, JmsPoolSessionProxy> sessionPool;
    private final JmsPoolSessionKey sessionKey;
    private final JMSVersionSupport versionSupport;

    // We either only create anonymous producers in which case these values will hold an instance
    // that we hand out to every caller as the sole JMS message producer in use be the pooled JMS
    // connection or we are creating explicitly bound JMS producers and these values are used as
    // a cache for any anonymous producers requested.
    private volatile JmsPoolMessageProducerProxy anonymousProducer;

    private final Map<Destination, JmsPoolMessageProducerProxy> cachedProducers;

    private volatile int closed = 1;

    public JmsPoolSessionProxy(JmsPoolConnectionProxy connection, JmsPoolSessionKey key, Session session, GenericKeyedObjectPool<JmsPoolSessionKey, JmsPoolSessionProxy> sessionPool) {
        this.configuration = connection.getConfiguration();
        this.versionSupport = connection.getVersionSupport();
        this.session = session;
        this.sessionPool = sessionPool;
        this.sessionKey = key;

        if (!configuration.isUseAnonymousProducers() && configuration.getExplicitProducerCacheSize() > 0) {
            cachedProducers = new ProducerLRUCache(configuration.getExplicitProducerCacheSize());
        } else {
            cachedProducers = DiscardingMap.getInstance();
        }
    }

    public void open() {
        if (!CLOSED_UPDATER.compareAndSet(this, 1, 0)) {
            throw new IllegalStateRuntimeException("Open called on an already opened or destroyed session, state should never mismatch");
        }
    }

    @Override
    public void close() {
        if (CLOSED_UPDATER.compareAndSet(this, 0, 1)) {
            sessionPool.returnObject(sessionKey, this);
        }
    }

    public void destroy() {
        if (CLOSED_UPDATER.compareAndSet(this, 0, 2)) {
            try {
                sessionPool.invalidateObject(sessionKey, this);
            } catch (Exception e) {
                LOG.trace("Ignoring exception on invalidateObject as discarding session: " + e.getMessage(), e);
            } finally {
                internalClose();
            }
        }
    }

    void internalClose() {
        try {
            session.close();
        } catch (JMSException ex) {
            LOG.trace("Ignoring exception while closing internal JMS session: " + ex.getMessage(), ex);
        } finally {
            anonymousProducer = null;
            cachedProducers.clear();
        }
    }

    public Session getProviderSession() throws JMSException {
        checkClosed();
        return session;
    }

    JMSVersionSupport getVersionSupport() {
        return versionSupport;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{ " + session + " ]";
    }

    protected synchronized JmsPoolMessageProducerProxy getOrCreateProducer(Destination destination) throws JMSException {
        JmsPoolMessageProducerProxy delegate = null;

        if (isUseAnonymousProducer() || destination == null) {
            delegate = anonymousProducer;

            if (delegate == null) {
                // Anonymous instance is not reference counted so create a fixed reference instance
                delegate = new JmsPoolMessageProducerProxy(this,
                                                           session.createProducer(null),
                                                           null,
                                                           new FixedReference(),
                                                           this::onProducerClosed,
                                                           this::onProducerDestroyed);

                anonymousProducer = delegate;
            }
        } else {
            delegate = cachedProducers.get(destination);

            if (delegate == null) {
                // Cached instance is held until evicted from the cache so take an additional reference on create.
                delegate = new JmsPoolMessageProducerProxy(this,
                                                           session.createProducer(destination),
                                                           destination,
                                                           new ReferenceCounted().acquire(),
                                                           this::onProducerClosed,
                                                           this::onProducerDestroyed);

                cachedProducers.put(destination, delegate);
            }
        }

        return delegate.acquire();
    }

    private synchronized void onProducerClosed(JmsPoolMessageProducerProxy producer, Destination destination) {
        // Nothing done here currently but we could use this to track statistics later.
    }

    private void onProducerDestroyed(JmsPoolMessageProducerProxy producer, Destination destination) {
        // Ensure that the anonymous reference are cleared of a destroyed producer resource or the
        // cache is updated if enabled to remove the destroyed named producer.
        if (producer == anonymousProducer) {
            anonymousProducer = null;
        } else {
            cachedProducers.remove(destination);
        }
    }

    private boolean isUseAnonymousProducer() {
        return configuration.isUseAnonymousProducers();
    }

    /**
     * Gets the state of the session closed flag.
     *
     * @return <code>true</code> if the session has been closed.
     */
    protected boolean isClosed() {
        return closed > 0;
    }

    /**
     * Checks for closure of this session wrapper and throws if true.
     *
     * @throws IllegalStateException if the session is closed.
     */
    protected void checkClosed() throws IllegalStateException {
        if (isClosed()) {
            throw new IllegalStateException("Connection has already been closed");
        }
    }

    protected void checkClosedRuntimeEx() throws IllegalStateRuntimeException {
        if (isClosed()) {
            throw new IllegalStateRuntimeException("Session has already been closed");
        }
    }

    private static class ProducerLRUCache extends LRUCache<Destination, JmsPoolMessageProducerProxy> {

        private static final long serialVersionUID = -1;

        public ProducerLRUCache(int maximumCacheSize) {
            super(maximumCacheSize);
        }

        @Override
        protected void onCacheEviction(Map.Entry<Destination, JmsPoolMessageProducerProxy> eldest) {
            // Closes the cache's reference to the producer which will close it fully
            // if the producer is not reference currently by any other client code.
            final JmsPoolMessageProducerProxy producer = eldest.getValue();
            try {
                producer.close();
            } catch (Exception ex) {}
        }
    }

    private static class DiscardingMap extends AbstractMap<Destination, JmsPoolMessageProducerProxy> {

        private static final DiscardingMap INSTANCE = new DiscardingMap();

        public static Map<Destination, JmsPoolMessageProducerProxy> getInstance() {
            return INSTANCE;
        }

        @Override
        public JmsPoolMessageProducerProxy put(Destination key, JmsPoolMessageProducerProxy value) {
            return null;
        }

        @Override
        public JmsPoolMessageProducerProxy get(Object key) {
            return null;
        }

        @Override
        public JmsPoolMessageProducerProxy remove(Object key) {
            return null;
        }

        @Override
        public void clear() {
            // Nothing to do here.
        }

        @Override
        public Set<Entry<Destination, JmsPoolMessageProducerProxy>> entrySet() {
            return Collections.emptySet();
        }
    }

    //----- JMS Message producer create APIs ---------------------------------//

    @Override
    public JmsPoolMessageProducerProxy createProducer(Destination destination) throws JMSException {
        checkClosed();
        return getOrCreateProducer(destination);
    }

    @Override
    public JmsPoolMessageProducerProxy createPublisher(Topic topic) throws JMSException {
        checkClosed();
        return getOrCreateProducer(topic);
    }

    @Override
    public JmsPoolMessageProducerProxy createSender(Queue queue) throws JMSException {
        checkClosed();
        return getOrCreateProducer(queue);
    }

    //----- JMS Message consumer create APIs ---------------------------------//

    @Override
    public JmsPoolMessageConsumerProxy createConsumer(Destination destination) throws JMSException {
        return createConsumer(destination, null, false);
    }

    @Override
    public JmsPoolMessageConsumerProxy createConsumer(Destination destination, String selector) throws JMSException {
        return createConsumer(destination, selector, false);
    }

    @Override
    public JmsPoolMessageConsumerProxy createConsumer(Destination destination, String selector, boolean noLocal) throws JMSException {
        checkClosed();
        return new JmsPoolMessageConsumerProxy(session.createConsumer(destination, selector, noLocal), destination, selector, noLocal);
    }

    @Override
    public JmsPoolMessageConsumerProxy createDurableConsumer(Topic topic, String name) throws JMSException {
        return createDurableConsumer(topic, name, null, false);
    }

    @Override
    public JmsPoolMessageConsumerProxy createDurableConsumer(Topic topic, String name, String selector, boolean noLocal) throws JMSException {
        checkClosed();
        return new JmsPoolMessageConsumerProxy(session.createDurableConsumer(topic, name, selector, noLocal), topic, selector, noLocal);
    }

    @Override
    public JmsPoolMessageConsumerProxy createSharedConsumer(Topic topic, String name) throws JMSException {
        return createSharedConsumer(topic, name, null);
    }

    @Override
    public JmsPoolMessageConsumerProxy createSharedConsumer(Topic topic, String name, String selector) throws JMSException {
        checkClosed();
        versionSupport.enforceSharedSubscriptionSupport();
        return new JmsPoolMessageConsumerProxy(session.createSharedConsumer(topic, name, selector), topic, selector, false);
    }

    @Override
    public JmsPoolMessageConsumerProxy createSharedDurableConsumer(Topic topic, String name) throws JMSException {
        checkClosed();
        versionSupport.enforceSharedSubscriptionSupport();
        return new JmsPoolMessageConsumerProxy(session.createSharedDurableConsumer(topic, name), topic, null, false);
    }

    @Override
    public JmsPoolMessageConsumerProxy createSharedDurableConsumer(Topic topic, String name, String selector) throws JMSException {
        checkClosed();
        versionSupport.enforceSharedSubscriptionSupport();
        return new JmsPoolMessageConsumerProxy(session.createSharedDurableConsumer(topic, name, selector), topic, selector, false);
    }

    @Override
    public JmsPoolMessageConsumerProxy createSubscriber(Topic topic) throws JMSException {
        return createSubscriber(topic, null, false);
    }

    @Override
    public JmsPoolMessageConsumerProxy createSubscriber(Topic topic, String selector, boolean noLocal) throws JMSException {
        checkClosed();
        return new JmsPoolMessageConsumerProxy(session.createConsumer(topic, selector, noLocal), topic, selector, noLocal);
    }

    @Override
    public JmsPoolMessageConsumerProxy createDurableSubscriber(Topic topic, String name) throws JMSException {
        return createDurableSubscriber(topic, name, null, false);
    }

    @Override
    public JmsPoolMessageConsumerProxy createDurableSubscriber(Topic topic, String name, String selector, boolean noLocal) throws JMSException {
        checkClosed();
        return new JmsPoolMessageConsumerProxy(session.createDurableConsumer(topic, name, selector, noLocal), topic, selector, noLocal);
    }

    @Override
    public JmsPoolMessageConsumerProxy createReceiver(Queue queue) throws JMSException {
        return createReceiver(queue, null);
    }

    @Override
    public JmsPoolMessageConsumerProxy createReceiver(Queue queue, String selector) throws JMSException {
        checkClosed();
        return new JmsPoolMessageConsumerProxy(session.createConsumer(queue, selector), queue, selector, false);
    }

    //----- JMS Queue Browser create APIs ------------------------------------//

    @Override
    public JmsPoolQueueBrowserProxy createBrowser(Queue queue) throws JMSException {
        checkClosed();
        return new JmsPoolQueueBrowserProxy(session.createBrowser(queue));
    }

    @Override
    public JmsPoolQueueBrowserProxy createBrowser(Queue queue, String messageSelector) throws JMSException {
        checkClosed();
        return new JmsPoolQueueBrowserProxy(session.createBrowser(queue, messageSelector));
    }

    //----- JMS Destination creation APIs ------------------------------------//

    @Override
    public Topic createTopic(String topicName) throws JMSException {
        checkClosed();
        return session.createTopic(topicName);
    }

    @Override
    public Queue createQueue(String queueName) throws JMSException {
        checkClosed();
        return session.createQueue(queueName);
    }

    @Override
    public TemporaryTopic createTemporaryTopic() throws JMSException {
        checkClosed();
        return session.createTemporaryTopic();
    }

    @Override
    public TemporaryQueue createTemporaryQueue() throws JMSException {
        checkClosed();
        return session.createTemporaryQueue();
    }

    //----- JMS Message creation APIs ----------------------------------------//

    @Override
    public BytesMessage createBytesMessage() throws JMSException {
        checkClosed();
        return session.createBytesMessage();
    }

    @Override
    public MapMessage createMapMessage() throws JMSException {
        checkClosed();
        return session.createMapMessage();
    }

    @Override
    public Message createMessage() throws JMSException {
        checkClosed();
        return session.createMessage();
    }

    @Override
    public ObjectMessage createObjectMessage() throws JMSException {
        checkClosed();
        return session.createObjectMessage();
    }

    @Override
    public ObjectMessage createObjectMessage(Serializable object) throws JMSException {
        checkClosed();
        return session.createObjectMessage(object);
    }

    @Override
    public StreamMessage createStreamMessage() throws JMSException {
        checkClosed();
        return session.createStreamMessage();
    }

    @Override
    public TextMessage createTextMessage() throws JMSException {
        checkClosed();
        return session.createTextMessage();
    }

    @Override
    public TextMessage createTextMessage(String text) throws JMSException {
        checkClosed();
        return session.createTextMessage(text);
    }

    //----- JMS Session interaction APIs -------------------------------------//

    @Override
    public boolean getTransacted() throws JMSException {
        checkClosed();
        return session.getTransacted();
    }

    @Override
    public int getAcknowledgeMode() throws JMSException {
        checkClosed();
        return session.getAcknowledgeMode();
    }

    @Override
    public void commit() throws JMSException {
        checkClosed();
        session.commit();
    }

    @Override
    public void rollback() throws JMSException {
        checkClosed();
        session.rollback();
    }

    @Override
    public void recover() throws JMSException {
        checkClosed();
        session.recover();
    }

    @Override
    public void run() {
        checkClosedRuntimeEx();
        session.run();
    }

    @Override
    public MessageListener getMessageListener() throws JMSException {
        checkClosed();
        return session.getMessageListener();
    }

    @Override
    public void setMessageListener(MessageListener listener) throws JMSException {
        checkClosed();
        session.setMessageListener(listener);
    }

    @Override
    public void unsubscribe(String name) throws JMSException {
        checkClosed();
        session.unsubscribe(name);
    }
}
