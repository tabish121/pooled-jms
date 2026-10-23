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

    private final Map<Destination, JmsMessageProducerProxy> cachedProducers;
    private final JmsSessionProxyManager<SP> manager;

    // We either only create anonymous producers in which case these values will hold an instance
    // that we hand out to every caller as the sole JMS message producer in use be the pooled JMS
    // connection or we are creating explicitly bound JMS producers and these values are used as
    // a cache for any anonymous producers requested.
    private volatile JmsMessageProducerProxy anonymousProducer;

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

        if (!configuration.isUseAnonymousProducers() && configuration.getExplicitProducerCacheSize() > 0) {
            cachedProducers = new ProducerLRUCache(configuration.getExplicitProducerCacheSize());
        } else {
            cachedProducers = DiscardingMap.getInstance();
        }
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
        return getOrCreateProducer(destination);
    }

    @Override
    public JmsMessageProducerProxy createPublisher(Topic topic) throws JMSException {
        checkClosed();
        return getOrCreateProducer(topic);
    }

    @Override
    public JmsMessageProducerProxy createSender(Queue queue) throws JMSException {
        checkClosed();
        return getOrCreateProducer(queue);
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
            anonymousProducer = null;
            cachedProducers.clear();
        }
    }

    boolean validateSession() {
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
                delegate = new JmsMessageProducerProxy(this,
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
                delegate = new JmsMessageProducerProxy(this,
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

    private synchronized void onProducerClosed(JmsMessageProducerProxy producer, Destination destination) {
        // Nothing done here currently but we could use this to track statistics later.
    }

    private void onProducerDestroyed(JmsMessageProducerProxy producer, Destination destination) {
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
        return closed;
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

    private static class ProducerLRUCache extends LRUCache<Destination, JmsMessageProducerProxy> {

        private static final long serialVersionUID = -1;

        public ProducerLRUCache(int maximumCacheSize) {
            super(maximumCacheSize);
        }

        @Override
        protected void onCacheEviction(Map.Entry<Destination, JmsMessageProducerProxy> eldest) {
            // Closes the cache's reference to the producer which will close it fully
            // if the producer is not reference currently by any other client code.
            final JmsMessageProducerProxy producer = eldest.getValue();
            try {
                producer.close();
            } catch (Exception ex) {}
        }
    }

    private static class DiscardingMap extends AbstractMap<Destination, JmsMessageProducerProxy> {

        private static final DiscardingMap INSTANCE = new DiscardingMap();

        public static Map<Destination, JmsMessageProducerProxy> getInstance() {
            return INSTANCE;
        }

        @Override
        public JmsMessageProducerProxy put(Destination key, JmsMessageProducerProxy value) {
            return null;
        }

        @Override
        public JmsMessageProducerProxy get(Object key) {
            return null;
        }

        @Override
        public JmsMessageProducerProxy remove(Object key) {
            return null;
        }

        @Override
        public void clear() {
            // Nothing to do here.
        }

        @Override
        public Set<Entry<Destination, JmsMessageProducerProxy>> entrySet() {
            return Collections.emptySet();
        }
    }
}
