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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import org.messaginghub.pooled.jms.util.FixedReference;
import org.messaginghub.pooled.jms.util.JMSVersionSupport;
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
 * When the client code closes the session it is returned to the pool of available
 * sessions which will be used during the next call to create a session from the
 * parent connection.
 *
 * @param <SP> the actual session proxy type that will be in use for this instance.
 */
public abstract class JmsAbstractSessionProxy<SP extends JmsAbstractSessionProxy<SP>> implements Session, TopicSession, QueueSession {

    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    protected final JmsConnectionProxyConfiguration configuration;
    protected final Session session;
    protected final JMSVersionSupport versionSupport;

    private final ProducerProxyManager producerManager;
    private final JmsSessionProxyManager<SP> manager;

    private volatile boolean closed;

    /**
     * Creates a new session proxy instance around a given provider JMS session
     *
     * @param configuration
     * 		The immutable configuration this session proxy operates under
     * @param versionSupprt
     * 		The version support type that defines what the provider connection can support
     * @param session
     * 		The provider JMS session taken from the session pool
     * @param manager
     * 		The manager who owns this JMS session proxy and receives its events
     */
    public JmsAbstractSessionProxy(JmsConnectionProxyConfiguration configuration,
                                   JMSVersionSupport versionSupprt,
                                   Session session,
                                   JmsSessionProxyManager<SP> manager) {
        this.configuration = configuration;
        this.versionSupport = versionSupprt;
        this.session = session;
        this.manager = manager;
        this.producerManager = new ProducerProxyManager(configuration);
    }

    /**
     * {@return the concrete session proxy instance}
     */
    protected abstract SP self();

    /**
     * Gets the underlying JMS provider Session instance if this proxy is not already closed.
     *
     * @return the proxied JMS {@link Session}
     *
     * @throws JMSException if the session proxy is already closed.
     */
    public Session getProviderSession() throws JMSException {
        checkClosed();
        return session;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{ " + session + " ]";
    }

    @Override
    public void close() {
        manager.releaseSession(self());
    }

    /**
     * Requests that the pool closes the underlying proxied session. This is generally used when
     * the caller knows that the state of the provider session is such that it should be closed
     * to allow a new clean instance to be created later.
     */
    public void destroy() {
        manager.closeSession(self());
    }

    //----- JMS Message producer create APIs ---------------------------------//

    @Override
    public JmsMessageProducerProxy createProducer(Destination destination) throws JMSException {
        checkClosed();
        return producerManager.getOrCreateProducer(destination);
    }

    @Override
    public JmsMessageProducerProxy createPublisher(Topic topic) throws JMSException {
        checkClosed();
        return producerManager.getOrCreateProducer(topic);
    }

    @Override
    public JmsMessageProducerProxy createSender(Queue queue) throws JMSException {
        checkClosed();
        return producerManager.getOrCreateProducer(queue);
    }

    //----- JMS Message consumer create APIs ---------------------------------//

    @Override
    public JmsMessageConsumerProxy createConsumer(Destination destination) throws JMSException {
        return createConsumer(destination, null, false);
    }

    @Override
    public JmsMessageConsumerProxy createConsumer(Destination destination, String selector) throws JMSException {
        return createConsumer(destination, selector, false);
    }

    @Override
    public JmsMessageConsumerProxy createConsumer(Destination destination, String selector, boolean noLocal) throws JMSException {
        checkClosed();
        return new JmsMessageConsumerProxy(session.createConsumer(destination, selector, noLocal), destination, selector, noLocal);
    }

    @Override
    public JmsMessageConsumerProxy createDurableConsumer(Topic topic, String name) throws JMSException {
        return createDurableConsumer(topic, name, null, false);
    }

    @Override
    public JmsMessageConsumerProxy createDurableConsumer(Topic topic, String name, String selector, boolean noLocal) throws JMSException {
        checkClosed();
        return new JmsMessageConsumerProxy(session.createDurableConsumer(topic, name, selector, noLocal), topic, selector, noLocal);
    }

    @Override
    public JmsMessageConsumerProxy createSharedConsumer(Topic topic, String name) throws JMSException {
        return createSharedConsumer(topic, name, null);
    }

    @Override
    public JmsMessageConsumerProxy createSharedConsumer(Topic topic, String name, String selector) throws JMSException {
        checkClosed();
        versionSupport.enforceSharedSubscriptionSupport();
        return new JmsMessageConsumerProxy(session.createSharedConsumer(topic, name, selector), topic, selector, false);
    }

    @Override
    public JmsMessageConsumerProxy createSharedDurableConsumer(Topic topic, String name) throws JMSException {
        checkClosed();
        versionSupport.enforceSharedSubscriptionSupport();
        return new JmsMessageConsumerProxy(session.createSharedDurableConsumer(topic, name), topic, null, false);
    }

    @Override
    public JmsMessageConsumerProxy createSharedDurableConsumer(Topic topic, String name, String selector) throws JMSException {
        checkClosed();
        versionSupport.enforceSharedSubscriptionSupport();
        return new JmsMessageConsumerProxy(session.createSharedDurableConsumer(topic, name, selector), topic, selector, false);
    }

    @Override
    public JmsMessageConsumerProxy createSubscriber(Topic topic) throws JMSException {
        return createSubscriber(topic, null, false);
    }

    @Override
    public JmsMessageConsumerProxy createSubscriber(Topic topic, String selector, boolean noLocal) throws JMSException {
        checkClosed();
        return new JmsMessageConsumerProxy(session.createConsumer(topic, selector, noLocal), topic, selector, noLocal);
    }

    @Override
    public JmsMessageConsumerProxy createDurableSubscriber(Topic topic, String name) throws JMSException {
        return createDurableSubscriber(topic, name, null, false);
    }

    @Override
    public JmsMessageConsumerProxy createDurableSubscriber(Topic topic, String name, String selector, boolean noLocal) throws JMSException {
        checkClosed();
        return new JmsMessageConsumerProxy(session.createDurableConsumer(topic, name, selector, noLocal), topic, selector, noLocal);
    }

    @Override
    public JmsMessageConsumerProxy createReceiver(Queue queue) throws JMSException {
        return createReceiver(queue, null);
    }

    @Override
    public JmsMessageConsumerProxy createReceiver(Queue queue, String selector) throws JMSException {
        checkClosed();
        return new JmsMessageConsumerProxy(session.createConsumer(queue, selector), queue, selector, false);
    }

    //----- JMS Queue Browser create APIs ------------------------------------//

    @Override
    public JmsQueueBrowserProxy createBrowser(Queue queue) throws JMSException {
        checkClosed();
        return new JmsQueueBrowserProxy(session.createBrowser(queue));
    }

    @Override
    public JmsQueueBrowserProxy createBrowser(Queue queue, String messageSelector) throws JMSException {
        checkClosed();
        return new JmsQueueBrowserProxy(session.createBrowser(queue, messageSelector));
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

    //----- Internal APIs not meant for public use

    void internalClose() {
        try {
            session.close();
        } catch (JMSException ex) {
            LOG.trace("Ignoring exception while closing internal JMS session: " + ex.getMessage(), ex);
        } finally {
            closed = true;
            producerManager.clear();
        }
    }

    boolean validateSession() {
        // The best we can do is check for closed JMS session as there is no viable means
        // of detecting failed underlying connections in JMS so this at least can capture
        // the case that the provider marks resources as closed if the underlying connection
        // enters a failed state.
        try {
            session.getTransacted();
        } catch (IllegalStateException jmsISE) {
            return false;
        } catch (Exception ambiguous) {
            // Unsure if session is still valid so continue as if it still is.
        }

        return true;
    }

    JMSVersionSupport getVersionSupport() {
        return versionSupport;
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
        return closed;
    }

    /**
     * Checks for closure of this session wrapper and throws if true.
     *
     * @throws IllegalStateException if the session is closed.
     */
    protected void checkClosed() throws IllegalStateException {
        if (isClosed()) {
            throw new IllegalStateException("Session has already been closed");
        }
    }

    /**
     * Checks for closure of this session wrapper and throws if true.
     *
     * @throws IllegalStateRuntimeException if the session is closed.
     */
    protected void checkClosedRuntimeEx() throws IllegalStateRuntimeException {
        if (isClosed()) {
            throw new IllegalStateRuntimeException("Session has already been closed");
        }
    }

    private class ProducerProxyManager implements JmsMessageProducerProxyManager {

        private final ProducerCache cachedProducers;

        // We either only create anonymous producers in which case these values will hold an instance
        // that we hand out to every caller as the sole JMS message producer in use be the pooled JMS
        // connection or we are creating explicitly bound JMS producers and this value
        private volatile JmsMessageProducerProxy anonymousProducer;

        public ProducerProxyManager(JmsConnectionProxyConfiguration configuration) {
            if (!configuration.isUseAnonymousProducers() && configuration.getExplicitProducerCacheSize() > 0) {
                cachedProducers = new ProducerLRUCache(configuration.getExplicitProducerCacheSize());
            } else {
                cachedProducers = DiscardingCache.getInstance();
            }
        }

        public synchronized void clear() {
            anonymousProducer = null;
            cachedProducers.clear();
        }

        @Override
        public synchronized JmsMessageProducerProxy tryReleaseReference(JmsMessageProducerProxy producer, Destination destination) {
            // Only cached producers ever become unreferenced so just clear the element from the cache in
            // that case to ensure it is gone. The fixed anonymous producer instance is given a reference
            // that can never be released.
            if (producer.releaseReference()) {
                cachedProducers.remove(destination);
                producer.doCloseProducer();
            }

            return producer;
        }

        @Override
        public synchronized JmsMessageProducerProxy destroyProducer(JmsMessageProducerProxy producer, Destination destination) {
            // Ensure that the anonymous reference are cleared of a destroyed producer resource or the
            // cache is updated if enabled to remove the destroyed named producer.
            if (producer == anonymousProducer) {
                anonymousProducer = null;
            } else {
                cachedProducers.remove(destination);
            }

            producer.doCloseProducer();

            return producer;
        }

        /**
         * Returns a JMS message producer proxy either from a cache or by creating a new instance
         *
         * @param destination
         * 		The destination the producer will send to (null if anonymous)
         *
         * @return a JMS message producer proxy that wraps the providers JMS message producer
         *
         * @throws JMSException if an error occurs while attempt to obtain a producer instance
         */
        protected synchronized JmsMessageProducerProxy getOrCreateProducer(Destination destination) throws JMSException {
            JmsMessageProducerProxy delegate = null;

            if (isUseAnonymousProducer() || destination == null) {
                delegate = anonymousProducer;

                if (delegate == null) {
                    // Anonymous instance is not reference counted so create a fixed reference instance
                    delegate = new JmsMessageProducerProxy(versionSupport,
                                                           session.createProducer(null),
                                                           null,
                                                           new FixedReference(),
                                                           this);

                    anonymousProducer = delegate;
                }
            } else {
                delegate = cachedProducers.get(destination);

                if (delegate == null) {
                    final ReferenceCounted refCounter = new ReferenceCounted();

                    delegate = new JmsMessageProducerProxy(versionSupport,
                                                           session.createProducer(destination),
                                                           destination,
                                                           refCounter,
                                                           this);

                    if (cachedProducers.put(destination, delegate)) {
                        // Cached instance is held until evicted from the cache so take an additional
                        // reference once we know it is in the cache
                        refCounter.acquire();
                    }
                }
            }

            return delegate.acquireReference();
        }
    }

    /**
     * Defines the interface for the internal JmsMessageProducerProxy cache that tracks a producer
     * mapped to a given destination. The implementation may hold a fixed set of producers or it may
     * hold none at all.
     *
     * These caches are not thread safe and must be protected by the object holding it.
     */
    private interface ProducerCache {

        /**
         * Put the given producer into the cache mapped to the supplied destination.
         * <p>
         * Neither the destination or the producer can be null even if the implementation does not
         * store any actual cached producers. In a storing implementation, once a producer is inserted
         * and if the result is that a previously stored value must be evicted then the method must
         * evict and release its reference on the stored producer by closing it.
         *
         * @param destination
         * 		The destination to store the producer at
         * @param producer
         * 		The producer proxy to store within the producer cache
         *
         * @return <code>true</code> if the value was stored in the cache, <code>false</code> otherwise
         */
        boolean put(Destination destination, JmsMessageProducerProxy producer);

        /**
         * Gets the JMS message producer proxy assigned to the given {@link Destination} key or
         * null if there is no producer currently assigned to that destination.
         *
         * @param destination
         * 		The {@link Destination} to lookup an associated producer
         *
         * @return the cached JMS producer proxy cached for the given destination
         */
        JmsMessageProducerProxy get(Destination destination);

        /**
         * Remove the JMS producer proxy stored at the given {@link Destination} in the mapping. If
         * the given destination is null no action is taken
         *
         * @param destination
         * 		The destination whose producer should be removed
         *
         * @return the previously stored producer proxy or null if not present
         */
        JmsMessageProducerProxy remove(Destination destination);

        /**
         * Discards all tracking data from this cache
         */
        void clear();

    }

    private static class ProducerLRUCache implements ProducerCache {

        private final Map<Destination, JmsMessageProducerProxy> cache;

        private final int maximumCacheSize;

        public ProducerLRUCache(int maximumCacheSize) {
            assert maximumCacheSize > 0;

            this.cache = new LinkedHashMap<>(Math.max(16, maximumCacheSize), 0.75f, true);
            this.maximumCacheSize = maximumCacheSize;
        }

        @Override
        public JmsMessageProducerProxy get(Destination destination) {
            return cache.get(destination);
        }

        @Override
        public boolean put(Destination destination, JmsMessageProducerProxy producer) {
            Objects.requireNonNull(destination, "Destination is not allowed to be null in this cache");
            Objects.requireNonNull(producer, "Cannot store a null producer proxy in a destination slot");

            JmsMessageProducerProxy evicted = null;

            if (cache.size() >= maximumCacheSize) {
                final Iterator<JmsMessageProducerProxy> values = cache.values().iterator();

                if (values.hasNext()) {
                    evicted = values.next();
                    values.remove();
                }
            }

            try {
                cache.put(destination, producer);
            } finally {
                try {
                    if (evicted != null) {
                        evicted.close();
                    }
                } catch (Exception ignored) {
                    LOG.trace("Suppressed error on cached producer close", ignored);
                }
            }

            return true;
        }

        @Override
        public JmsMessageProducerProxy remove(Destination destination) {
            if (destination != null) {
                return cache.remove(destination);
            } else {
                return null;
            }
        }

        @Override
        public void clear() {
            if (cache.isEmpty()) {
                return;
            }

            final Collection<JmsMessageProducerProxy> values = new ArrayList<JmsMessageProducerProxy>(cache.values());

            cache.clear();

            values.forEach(producer -> {
                try {
                    producer.close();
                } catch (Exception ignored) {
                    LOG.trace("Suppressed error on cached producer close", ignored);
                }
            });
        }
    }

    private static class DiscardingCache implements ProducerCache {

        private static final DiscardingCache INSTANCE = new DiscardingCache();

        public static ProducerCache getInstance() {
            return INSTANCE;
        }

        @Override
        public JmsMessageProducerProxy get(Destination destination) {
            return null;
        }

        @Override
        public boolean put(Destination destination, JmsMessageProducerProxy producer) {
            Objects.requireNonNull(destination, "Destination is not allowed to be null in this cache");
            Objects.requireNonNull(producer, "Cannot store a null producer proxy in a destination slot");

            return false;
        }

        @Override
        public JmsMessageProducerProxy remove(Destination destination) {
            return null;
        }

        @Override
        public void clear() {
            // Nothing to do
        }
    }
}
