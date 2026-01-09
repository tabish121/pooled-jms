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

import java.util.AbstractMap;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.messaginghub.pooled.jms.util.LRUCache;

import jakarta.jms.Destination;
import jakarta.jms.IllegalStateException;
import jakarta.jms.JMSException;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.QueueSender;
import jakarta.jms.QueueSession;
import jakarta.jms.Session;
import jakarta.jms.Topic;
import jakarta.jms.TopicPublisher;
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
final class JmsPoolSharedSession {

    private final JmsPoolSharedConnection connection;
    private final JmsPoolConnectionConfiguration configuration;
    private final Session session;

    private volatile MessageProducer anonymousProducer;
    private volatile TopicPublisher anonymousPublisher;
    private volatile QueueSender anonymousSender;

    private final Map<Destination, JmsPoolMessageProducer> cachedProducers;
    private final Map<Destination, JmsPoolTopicPublisher> cachedPublishers;
    private final Map<Destination, JmsPoolQueueSender> cachedSenders;

    public JmsPoolSharedSession(JmsPoolSharedConnection connection, JmsPoolConnectionConfiguration configuration, Session session) {
        this.configuration = configuration;
        this.connection = connection;
        this.session = session;

        if (!configuration.isUseAnonymousProducers() && configuration.getExplicitProducerCacheSize() > 0) {
            cachedProducers = new ProducerLRUCache<>(configuration.getExplicitProducerCacheSize());
            cachedPublishers = new ProducerLRUCache<>(configuration.getExplicitProducerCacheSize());
            cachedSenders = new ProducerLRUCache<>(configuration.getExplicitProducerCacheSize());
        } else {
            cachedProducers = DiscardingMap.getInstance();
            cachedPublishers = DiscardingMap.getInstance();
            cachedSenders = DiscardingMap.getInstance();
        }
    }

    public void close() throws JMSException {
        try {
            session.close();
        } finally {
            anonymousProducer = null;
            anonymousPublisher = null;
            anonymousSender = null;
            cachedProducers.clear();
            cachedPublishers.clear();
            cachedSenders.clear();
        }
    }

    public Session getSession() {
        return session;
    }

    private boolean shouldCleanupProducer(JmsPoolMessageProducer producer, boolean force) {
        // We cache anonymous producers regardless of the useAnonymousProducer
        // setting so in either of those cases the pooled producer is not closed
        // unless the wrapper indicates a forced closure is being requested.
        if (!force) {
            try {
                // Try and test the JMS resource to validate if it is still active.
                producer.getDelegate().getDestination();

                if (!isUseAnonymousProducer() && !producer.isAnonymousProducer()) {
                    force = producer.getRefCount().decrementAndGet() <= 0;
                }
            } catch (IllegalStateException jmsISE) {
                // Delegated producer appears to be closed so remove it from pooling.
                producer.getRefCount().decrementAndGet();
                force = true;
            } catch (Exception ambiguous) {
                // Not clear that the resource is closed so we don't assume it is.
            }
        }

        return force;
    }

    public void onJmsPoolProducerClosed(JmsPoolMessageProducer producer, boolean force) throws JMSException {
        synchronized (this) {
            // We cache anonymous producers regardless of the useAnonymousProducer
            // setting so in either of those cases the pooled producer is not closed
            // unless the wrapper indicates a forced closure is being requested.
            if (shouldCleanupProducer(producer, force)) {
                final MessageProducer delegate = producer.getDelegate();

                // Ensure that the anonymous reference are cleared of a closed producer resource or the
                // cache is updated if enabled to remove the closed named producer.

                if (producer.isTopicPublisher()) {
                    if (delegate == anonymousPublisher) {
                        anonymousPublisher = null;
                    } else {
                        cachedPublishers.remove(producer.getDelegateDestination());
                    }
                } else if (producer.isQueueSender()) {
                    if (delegate == anonymousSender) {
                        anonymousSender = null;
                    } else {
                        cachedSenders.remove(producer.getDelegateDestination());
                    }
                } else {
                    if (delegate == anonymousProducer) {
                        anonymousProducer = null;
                    } else {
                        cachedProducers.remove(producer.getDelegateDestination());
                    }
                }

                delegate.close();
            }
        }
    }

    public JmsPoolMessageProducer getOrCreateProducer(JmsPoolSession jmsPoolSession, Destination destination) throws JMSException {
        MessageProducer delegate = null;
        AtomicInteger refCount = null;

        synchronized (this) {
            if (isUseAnonymousProducer() || destination == null) {
                delegate = anonymousProducer;
                if (delegate == null) {
                    delegate = anonymousProducer = session.createProducer(null);
                    refCount = new AtomicInteger(0); // Anonymous instance is not reference counted
                }
            } else {
                JmsPoolMessageProducer cached = cachedProducers.get(destination);
                if (cached == null) {
                    delegate = session.createProducer(destination);
                    refCount = new AtomicInteger(1); // Cached instance is held as a reference until evicted
                    cached = new JmsPoolMessageProducer(jmsPoolSession, delegate, destination, refCount);

                    cachedProducers.put(destination, cached);
                } else {
                    delegate = cached.getDelegate();
                    refCount = cached.getRefCount();
                }

                refCount.incrementAndGet();
            }
        }

        return new JmsPoolMessageProducer(jmsPoolSession, delegate, destination, refCount);
    }

    public JmsPoolTopicPublisher getOrCreatePublisher(JmsPoolSession jmsPoolSession, Topic topic) throws JMSException {
        TopicPublisher delegate = null;
        AtomicInteger refCount = null;

        synchronized (this) {
            if (isUseAnonymousProducer() || topic == null) {
                delegate = anonymousPublisher;
                if (delegate == null) {
                    delegate = anonymousPublisher = ((TopicSession) session).createPublisher(null);
                    refCount = new AtomicInteger(0); // Anonymous instance is not reference counted
                }
            } else {
                JmsPoolTopicPublisher cached = cachedPublishers.get(topic);
                if (cached == null) {
                    delegate = ((TopicSession) session).createPublisher(topic);
                    refCount = new AtomicInteger(1); // Cached instance is held as a reference until evicted
                    cached = new JmsPoolTopicPublisher(jmsPoolSession, delegate, topic, refCount);

                    cachedPublishers.put(topic, cached);
                } else {
                    delegate = (TopicPublisher) cached.getDelegate();
                    refCount = cached.getRefCount();
                }

                refCount.incrementAndGet();
            }
        }

        return new JmsPoolTopicPublisher(jmsPoolSession, delegate, topic, refCount);
    }

    public JmsPoolQueueSender getOrCreateSender(JmsPoolSession jmsPoolSession, Queue queue) throws JMSException {
        QueueSender delegate = null;
        AtomicInteger refCount = null;

        synchronized (this) {
            if (isUseAnonymousProducer() || queue == null) {
                delegate = anonymousSender;
                if (delegate == null) {
                    delegate = anonymousSender = ((QueueSession) session).createSender(null);
                    refCount = new AtomicInteger(0); // Anonymous instance is not reference counted
                }
            } else {
                JmsPoolQueueSender cached = cachedSenders.get(queue);
                if (cached == null) {
                    delegate = ((QueueSession) session).createSender(queue);
                    refCount = new AtomicInteger(1); // Cached instance is held as a reference until evicted
                    cached = new JmsPoolQueueSender(jmsPoolSession, delegate, queue, refCount);

                    cachedSenders.put(queue, cached);
                } else {
                    delegate = (QueueSender) cached.getDelegate();
                    refCount = cached.getRefCount();
                }

                refCount.incrementAndGet();
            }
        }

        return new JmsPoolQueueSender(jmsPoolSession, delegate, queue, refCount);
    }

    public JmsPoolSharedConnection getConnection() {
        return connection;
    }

    public boolean isUseAnonymousProducer() {
        return configuration.isUseAnonymousProducers();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{ " + session + " ]";
    }

    private static class ProducerLRUCache<E> extends LRUCache<Destination, E> {

        private static final long serialVersionUID = -1;

        public ProducerLRUCache(int maximumCacheSize) {
            super(maximumCacheSize);
        }

        @Override
        protected void onCacheEviction(Map.Entry<Destination, E> eldest) {
            // Closes the cache's reference to the producer which will close it fully
            // if the producer is not reference currently by any other client code.
            JmsPoolMessageProducer producer = (JmsPoolMessageProducer) eldest.getValue();
            try {
                producer.close();
            } catch (JMSException jmsEx) {}
        }
    }

    private static class DiscardingMap<E> extends AbstractMap<Destination, E> {

        private static final DiscardingMap<Object> INSTANCE = new DiscardingMap<>();

        @SuppressWarnings("unchecked")
        public static <V> Map<Destination, V> getInstance() {
            return (Map<Destination, V>) INSTANCE;
        }

        @Override
        public E put(Destination key, E value) {
            return null;
        }

        @Override
        public E get(Object key) {
            return null;
        }

        @Override
        public E remove(Object key) {
            return null;
        }

        @Override
        public void clear() {
            // Nothing to do here.
        }

        @Override
        public Set<Entry<Destination, E>> entrySet() {
            return Collections.emptySet();
        }
    }
}
