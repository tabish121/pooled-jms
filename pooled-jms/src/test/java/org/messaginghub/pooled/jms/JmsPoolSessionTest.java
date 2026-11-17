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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.messaginghub.pooled.jms.internal.JmsAbstractSessionPool;
import org.messaginghub.pooled.jms.mock.MockJMSConnection;
import org.messaginghub.pooled.jms.mock.MockJMSDefaultConnectionListener;
import org.messaginghub.pooled.jms.mock.MockJMSDefaultSessionListener;
import org.messaginghub.pooled.jms.mock.MockJMSMessageProducer;
import org.messaginghub.pooled.jms.mock.MockJMSQueue;
import org.messaginghub.pooled.jms.mock.MockJMSSession;
import org.messaginghub.pooled.jms.mock.MockJMSTemporaryQueue;
import org.messaginghub.pooled.jms.mock.MockJMSTemporaryTopic;
import org.messaginghub.pooled.jms.mock.MockJMSTopic;

import jakarta.jms.IllegalStateException;
import jakarta.jms.IllegalStateRuntimeException;
import jakarta.jms.JMSException;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.Message;
import jakarta.jms.MessageListener;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.QueueSession;
import jakarta.jms.ResourceAllocationException;
import jakarta.jms.Session;
import jakarta.jms.TemporaryQueue;
import jakarta.jms.TemporaryTopic;
import jakarta.jms.Topic;
import jakarta.jms.TopicSession;
import jakarta.jms.XASession;

@Timeout(60)
public class JmsPoolSessionTest extends JmsPoolTestSupport {

    @Test
    public void testToString() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session.toString());
        session.close();
        assertNotNull(session.toString());
    }

    @Test
    public void testRun() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        try {
            session.run();
            fail("Session should be unable to run outside EE.");
        } catch (JMSRuntimeException jmsre) {}

        session.close();

        try {
            session.run();
            fail("Session should be closed.");
        } catch (IllegalStateRuntimeException isre) {}
    }

    @Test
    public void testClose() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        assertEquals(0, connection.getNumIdleSessions());
        session.close();
        assertEquals(1, connection.getNumIdleSessions());

        try {
            session.close();
        } catch (JMSException ex) {
            fail("Shouldn't fail on second close call.");
        }

        try {
            session.createTemporaryQueue();
            fail("Session should be closed.");
        } catch (IllegalStateException ise) {}
    }

    @Test
    public void testCloseOnTXSessionTriggersRollback() throws Exception {
        final AtomicBoolean rolledBack = new AtomicBoolean();

        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        JmsPoolSession session = (JmsPoolSession) connection.createSession(Session.SESSION_TRANSACTED);
        MockJMSSession mockSession = (MockJMSSession) session.getProviderSession();
        mockSession.addSessionListener(new MockJMSDefaultSessionListener() {

            @Override
            public void onSessionRollback(MockJMSSession session) throws JMSException {
                rolledBack.set(true);
            }
        });

        session.close();
        assertTrue(rolledBack.get(), "Session should rollback on close");
    }

    @Test
    public void testCloseOnClientAckSessionTriggersRecover() throws Exception {
        final AtomicBoolean recovered = new AtomicBoolean();

        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        JmsPoolSession session = (JmsPoolSession) connection.createSession(Session.CLIENT_ACKNOWLEDGE);
        MockJMSSession mockSession = (MockJMSSession) session.getProviderSession();
        mockSession.addSessionListener(new MockJMSDefaultSessionListener() {

            @Override
            public void onSessionRecover(MockJMSSession session) throws JMSException {
                recovered.set(true);
            }
        });

        session.close();
        assertTrue(recovered.get(), "Session should recover on close");
    }

    @Test
    public void testCloseWithErrorOnRollbackInvalidatesSession() throws Exception {
        final AtomicBoolean rolledBack = new AtomicBoolean();
        final AtomicBoolean closed = new AtomicBoolean();

        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        JmsPoolSession session = (JmsPoolSession) connection.createSession(Session.SESSION_TRANSACTED);

        assertEquals(1, connection.getNumSessions());
        assertEquals(0, connection.getNumIdleSessions());

        MockJMSSession mockSession = (MockJMSSession) session.getProviderSession();
        mockSession.addSessionListener(new MockJMSDefaultSessionListener() {

            @Override
            public void onSessionRollback(MockJMSSession session) throws JMSException {
                rolledBack.set(true);

                throw new JMSException("Failed to rollback");
            }

            @Override
            public void onSessionClosed(MockJMSSession session) throws JMSException {
                closed.set(true);
            }
        });

        session.close();

        assertTrue(closed.get(), "Session close should have been called");
        assertTrue(rolledBack.get(), "Session should rollback on close");

        assertEquals(0, connection.getNumSessions());
        assertEquals(0, connection.getNumIdleSessions());
    }

    @Test
    public void testConnectionCloseReflectedInSessionState() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        connection.close();

        try {
            session.getAcknowledgeMode();
            fail("Session should be closed.");
        } catch (IllegalStateException isre) {}

        try {
            session.run();
            fail("Session should be closed.");
        } catch (IllegalStateRuntimeException isre) {}

        try {
            session.createTemporaryQueue();
            fail("Session should be closed.");
        } catch (IllegalStateException ise) {}
    }

    @Test
    public void testCreateQueue() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getTestName());
        assertNotNull(queue);
        assertEquals(getTestName(), queue.getQueueName());
        assertTrue(queue instanceof MockJMSQueue);
    }

    @Test
    public void testCreateTopic() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic(getTestName());
        assertNotNull(topic);
        assertEquals(getTestName(), topic.getTopicName());
        assertTrue(topic instanceof MockJMSTopic);
    }

    @Test
    public void testCreateTemporaryQueue() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TemporaryQueue queue = session.createTemporaryQueue();
        assertNotNull(queue);
        assertTrue(queue instanceof MockJMSTemporaryQueue);
    }

    @Test
    public void testCreateTemporaryTopic() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TemporaryTopic topic = session.createTemporaryTopic();
        assertNotNull(topic);
        assertTrue(topic instanceof MockJMSTemporaryTopic);
    }

    @Test
    public void testGetXAResourceOnNonXAPooledSession() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        JmsPoolSession session = (JmsPoolSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertFalse(session instanceof XASession);
    }

    @Test
    public void testPooledSessionStatsOneSession() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();

        assertEquals(0, connection.getNumActiveSessions());

        // Create one and check that stats follow
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertEquals(1, connection.getNumActiveSessions());
        session.close();

        // All back in the pool now
        assertEquals(0, connection.getNumActiveSessions());
        assertEquals(1, connection.getNumIdleSessions());
        assertEquals(1, connection.getNumSessions());

        connection.close();
    }

    @Test
    public void testPooledSessionStatsOneSessionWithSessionLimit() throws Exception {
        cf.setMaxSessionsPerConnection(1);
        cf.setBlockIfSessionPoolIsFull(false);

        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();

        assertEquals(0, connection.getNumActiveSessions());

        // Create one and check that stats follow
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertEquals(1, connection.getNumActiveSessions());
        assertEquals(0, connection.getNumIdleSessions());
        assertEquals(1, connection.getNumSessions());

        try {
            connection.createSession();
            fail("Should not be able to create new session");
        } catch (ResourceAllocationException ise) {}

        // Nothing should have changed as we didn't create anything.
        assertEquals(1, connection.getNumActiveSessions());
        assertEquals(0, connection.getNumIdleSessions());
        assertEquals(1, connection.getNumSessions());

        session.close();

        // All back in the pool now
        assertEquals(0, connection.getNumActiveSessions());
        assertEquals(1, connection.getNumIdleSessions());
        assertEquals(1, connection.getNumSessions());

        connection.close();
    }

    @Test
    public void testPooledSessionStatsTwoSessions() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();

        assertEquals(0, connection.getNumActiveSessions());

        // Create Two and check that stats follow
        Session session1 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Session session2 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertEquals(2, connection.getNumActiveSessions());
        session1.close();
        assertEquals(1, connection.getNumActiveSessions());
        session2.close();

        // All back in the pool now.
        assertEquals(0, connection.getNumActiveSessions());
        assertEquals(2, connection.getNumIdleSessions());
        assertEquals(2, connection.getNumSessions());

        connection.close();
    }

    @Test
    public void testPooledSessionStatsTenSessions() throws Exception {
        final int NUM_SESSIONS = 10;

        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        assertEquals(0, connection.getNumActiveSessions());
        List<Session> sessions = new ArrayList<>();

        for (int i = 0; i < NUM_SESSIONS; i++) {
            sessions.add(connection.createSession(false, Session.AUTO_ACKNOWLEDGE));
        }

        assertEquals(NUM_SESSIONS, connection.getNumActiveSessions());

        for (int i = 0; i < NUM_SESSIONS; i++) {
            sessions.get(i).close();
        }

        // All back in the pool now.
        assertEquals(0, connection.getNumActiveSessions());
        assertEquals(NUM_SESSIONS, connection.getNumIdleSessions());
        assertEquals(NUM_SESSIONS, connection.getNumSessions());

        connection.close();
    }

    @Test
    public void testPooledSessionsClosedIfMaxIdleExceeded() throws Exception {
        final int NUM_SESSIONS = 10;
        final int NUM_IDLE_SESSIONS = 1;

        // Configure max and idle to check that on all returned only the idle
        // max amount of sessions remains.
        cf.setMaxSessionsPerConnection(NUM_SESSIONS);
        cf.setMaxIdleSessionsPerConnection(NUM_IDLE_SESSIONS);

        assertEquals(NUM_SESSIONS, cf.getMaxSessionsPerConnection());
        assertEquals(NUM_IDLE_SESSIONS, cf.getMaxIdleSessionsPerConnection());

        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        assertEquals(0, connection.getNumActiveSessions());
        List<Session> sessions = new ArrayList<>();

        for (int i = 0; i < NUM_SESSIONS; i++) {
            sessions.add(connection.createSession(false, Session.AUTO_ACKNOWLEDGE));
        }

        assertEquals(NUM_SESSIONS, connection.getNumActiveSessions());

        for (int i = 0; i < NUM_SESSIONS; i++) {
            sessions.get(i).close();
        }

        // All back in the pool now.
        assertEquals(0, connection.getNumActiveSessions());
        assertEquals(NUM_IDLE_SESSIONS, connection.getNumIdleSessions());
        assertEquals(NUM_IDLE_SESSIONS, connection.getNumSessions());

        connection.close();
    }

    @Test
    public void testPooledSessionsIdleAmountIsCappedAtMaxSession() throws Exception {
        final int NUM_SESSIONS = 10;
        final int NUM_IDLE_SESSIONS = 20;

        // We should never allow more than max session regardless of faulty configured
        // max idle sessions
        cf.setMaxSessionsPerConnection(NUM_SESSIONS);
        cf.setMaxIdleSessionsPerConnection(NUM_IDLE_SESSIONS);
        cf.setBlockIfSessionPoolIsFull(false);

        assertFalse(cf.isBlockIfSessionPoolIsFull());

        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        assertEquals(0, connection.getNumActiveSessions());
        List<Session> sessions = new ArrayList<>();

        for (int i = 0; i < NUM_SESSIONS; i++) {
            sessions.add(connection.createSession(false, Session.AUTO_ACKNOWLEDGE));
        }

        assertEquals(NUM_SESSIONS, connection.getNumActiveSessions());

        assertThrows(ResourceAllocationException.class, () -> connection.createSession(false, Session.AUTO_ACKNOWLEDGE));

        assertEquals(NUM_SESSIONS, connection.getNumActiveSessions());

        for (int i = 0; i < NUM_SESSIONS; i++) {
            sessions.get(i).close();
        }

        // All back in the pool now.
        assertEquals(0, connection.getNumActiveSessions());
        assertEquals(NUM_SESSIONS, connection.getNumIdleSessions());
        assertEquals(NUM_SESSIONS, connection.getNumSessions());

        connection.close();
    }

    @Test
    public void testAddSessionEventListener() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        JmsPoolSession session = (JmsPoolSession) connection.createSession();

        final CountDownLatch tempTopicCreated = new CountDownLatch(2);
        final CountDownLatch tempQueueCreated = new CountDownLatch(2);
        final CountDownLatch sessionClosed = new CountDownLatch(2);

        JmsPoolSessionEventListener listener = new JmsPoolSessionEventListener() {

            @Override
            public void onTemporaryTopicCreate(TemporaryTopic tempTopic) {
                tempTopicCreated.countDown();
            }

            @Override
            public void onTemporaryQueueCreate(TemporaryQueue tempQueue) {
                tempQueueCreated.countDown();
            }

            @Override
            public void onSessionClosed(Session session) {
                sessionClosed.countDown();
            }
        };

        session.addSessionEventListener(listener);
        session.addSessionEventListener(listener);

        assertNotNull(session.createTemporaryQueue());
        assertNotNull(session.createTemporaryTopic());

        session.close();

        assertEquals(1, tempQueueCreated.getCount());
        assertEquals(1, tempTopicCreated.getCount());
        assertEquals(1, sessionClosed.getCount());

        try {
            session.addSessionEventListener(listener);
            fail("Should throw on closed session.");
        } catch (IllegalStateException ise) {}
    }

    @Test
    public void testCreateDurableSubscriber() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        JmsPoolSession session = (JmsPoolSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Topic topic = session.createTopic(getTestName());
        assertNotNull(session.createDurableSubscriber(topic, "name"));

        session.close();
        try {
            session.createDurableSubscriber(topic, "name-2");
            fail("Should not be able to createDurableSubscriber when closed");
        } catch (JMSException ex) {}
    }

    @Test
    public void testCreateDurableSubscriberWithSelectorAndNoLocal() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        JmsPoolSession session = (JmsPoolSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Topic topic = session.createTopic(getTestName());
        assertNotNull(session.createDurableSubscriber(topic, "name", "color = red", false));

        session.close();
        try {
            session.createDurableSubscriber(topic, "other-name", "color = green", true);
            fail("Should not be able to createDurableSubscriber when closed");
        } catch (JMSException ex) {}
    }

    @Test
    public void testCreateSubscriber() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        JmsPoolSession session = (JmsPoolSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Topic topic = session.createTopic(getTestName());
        assertNotNull(session.createSubscriber(topic));

        session.close();
        try {
            session.createSubscriber(topic);
            fail("Should not be able to createSubscriber when closed");
        } catch (JMSException ex) {}
    }

    @Test
    public void testCreateSubscriberWithSelectorAndNoLocal() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        JmsPoolSession session = (JmsPoolSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Topic topic = session.createTopic(getTestName());
        assertNotNull(session.createSubscriber(topic, "color = red", false));

        session.close();
        try {
            session.createSubscriber(topic, "color = green", true);
            fail("Should not be able to createSubscriber when closed");
        } catch (JMSException ex) {}
    }

    @Test
    public void testCreateReceiver() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        JmsPoolSession session = (JmsPoolSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Queue queue = session.createQueue(getTestName());
        assertNotNull(session.createReceiver(queue));

        session.close();
        try {
            session.createReceiver(queue);
            fail("Should not be able to createReceiver when closed");
        } catch (JMSException ex) {}
    }

    @Test
    public void testCreateReceiverWithSelector() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        JmsPoolSession session = (JmsPoolSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Queue queue = session.createQueue(getTestName());
        assertNotNull(session.createReceiver(queue, "color = red"));

        session.close();
        try {
            session.createReceiver(queue, "color = green");
            fail("Should not be able to createReceiver when closed");
        } catch (JMSException ex) {}
    }

    @Test
    public void testSetMessageListener() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        JmsPoolSession session = (JmsPoolSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageListener listener = new MessageListener() {

            @Override
            public void onMessage(Message message) {
            }
        };

        session.setMessageListener(listener);
        assertSame(listener, session.getMessageListener());
        MockJMSSession mockSession = (MockJMSSession) session.getProviderSession();
        assertSame(listener, mockSession.getMessageListener());

        session.close();

        try {
            session.setMessageListener(new MessageListener() {

                @Override
                public void onMessage(Message message) {
                }
            });
            fail("Should not be able to setMessageListener when closed");
        } catch (JMSException ex) {}

        try {
            session.getMessageListener();
            fail("Should not be able to setMessageListener when closed");
        } catch (JMSException ex) {}
    }

    @Test
    public void testCachedProducersEnabledReturnsCached() throws Exception {
        cf.setUseAnonymousProducers(false);
        cf.setExplicitProducerCacheSize(2);

        assertFalse(cf.isUseAnonymousProducers());
        assertEquals(2, cf.getExplicitProducerCacheSize());

        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Queue queue1 = session.createTemporaryQueue();
        Queue queue2 = session.createTemporaryQueue();

        JmsPoolMessageProducer producer1 = (JmsPoolMessageProducer) session.createProducer(queue1);
        JmsPoolMessageProducer producer2 = (JmsPoolMessageProducer) session.createProducer(queue2);

        assertNotSame(producer1.getProviderMessageProducer(), producer2.getProviderMessageProducer());

        JmsPoolMessageProducer producer3 = (JmsPoolMessageProducer) session.createProducer(queue1);
        JmsPoolMessageProducer producer4 = (JmsPoolMessageProducer) session.createProducer(queue2);

        assertSame(producer1.getProviderMessageProducer(), producer3.getProviderMessageProducer());
        assertSame(producer2.getProviderMessageProducer(), producer4.getProviderMessageProducer());

        connection.close();
    }

    @Test
    public void testCachedProducersEnabledReturnsCachedOnNewSession() throws Exception {
        cf.setUseAnonymousProducers(false);
        cf.setExplicitProducerCacheSize(1);

        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Queue queue1 = session.createTemporaryQueue();

        JmsPoolMessageProducer producer1 = (JmsPoolMessageProducer) session.createProducer(queue1);
        MessageProducer providerProducer1 = producer1.getProviderMessageProducer();

        session.close();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // Should return a wrapper whose underlying MessageProducer matches what we got from
        // the first session as they underlying session will be the same.
        JmsPoolMessageProducer producer2 = (JmsPoolMessageProducer) session.createProducer(queue1);

        assertSame(providerProducer1, producer2.getProviderMessageProducer());

        connection.close();
    }

    @Test
    public void testCachedProducersEnabledEvictsOldAndCreatesNew() throws Exception {
        cf.setUseAnonymousProducers(false);
        cf.setExplicitProducerCacheSize(2);

        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Queue queue1 = session.createTemporaryQueue();
        Queue queue2 = session.createTemporaryQueue();
        Queue queue3 = session.createTemporaryQueue();

        JmsPoolMessageProducer producer1 = (JmsPoolMessageProducer) session.createProducer(queue1);
        JmsPoolMessageProducer producer2 = (JmsPoolMessageProducer) session.createProducer(queue2);

        assertNotSame(producer1.getProviderMessageProducer(), producer2.getProviderMessageProducer());

        JmsPoolMessageProducer producer3 = (JmsPoolMessageProducer) session.createProducer(queue3);
        JmsPoolMessageProducer producer4 = (JmsPoolMessageProducer) session.createProducer(queue1);

        assertNotSame(producer1.getProviderMessageProducer(), producer3.getProviderMessageProducer());
        assertNotSame(producer2.getProviderMessageProducer(), producer3.getProviderMessageProducer());

        // Original MessageProducer should have been evicted from the cache and a new one created
        // for the second call to create a producer on Queue #1
        assertNotSame(producer1.getProviderMessageProducer(), producer4.getProviderMessageProducer());

        JmsPoolMessageProducer producer5 = (JmsPoolMessageProducer) session.createProducer(queue1);

        // Now we should be back to caching Queue #1
        assertNotSame(producer1.getProviderMessageProducer(), producer5.getProviderMessageProducer());

        connection.close();
    }

    @Test
    public void testCachedProducersEvictedFromCacheNotClosedUntilAllReferencesAreClosed() throws Exception {
        cf.setUseAnonymousProducers(false);
        cf.setExplicitProducerCacheSize(1);

        final AtomicBoolean producerClosed = new AtomicBoolean();

        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Queue queue1 = session.createTemporaryQueue();
        Queue queue2 = session.createTemporaryQueue();

        final JmsPoolMessageProducer producer1 = (JmsPoolMessageProducer) session.createProducer(queue1);
        final MockJMSMessageProducer mockProducer1 = (MockJMSMessageProducer) producer1.getProviderMessageProducer();
        final JmsPoolMessageProducer producer2 = (JmsPoolMessageProducer) session.createProducer(queue1);
        final MockJMSMessageProducer mockProducer2 = (MockJMSMessageProducer) producer2.getProviderMessageProducer();

        MockJMSConnection mockConnection = (MockJMSConnection) connection.getProviderConnection();
        mockConnection.addConnectionListener(new MockJMSDefaultConnectionListener() {

            @Override
            public void onCloseMessageProducer(MockJMSSession session, MockJMSMessageProducer producer) throws JMSException {
                if (producer.equals(mockProducer1)) {
                    producerClosed.set(true);
                }
            }
        });

        assertSame(producer1.getProviderMessageProducer(), producer2.getProviderMessageProducer());
        assertSame(mockProducer1, mockProducer2);

        // This replaces cached producer
        JmsPoolMessageProducer producer3 = (JmsPoolMessageProducer) session.createProducer(queue2);

        assertNotSame(producer1.getProviderMessageProducer(), producer3.getProviderMessageProducer());

        try {
            producer1.send(session.createMessage());
        } catch (JMSException ex) {
            fail("Should be able to send on evicted producer");
        }

        try {
            producer2.send(session.createMessage());
        } catch (JMSException ex) {
            fail("Should be able to send on evicted producer");
        }

        producer1.close();
        assertFalse(producerClosed.get());

        try {
            producer1.send(session.createMessage());
            fail("Should not be able to send on closed evicted producer");
        } catch (JMSException ex) {
        }

        try {
            producer2.send(session.createMessage());
        } catch (JMSException ex) {
            fail("Should be able to send on alternate evicted producer");
        }

        producer2.close();
        assertTrue(producerClosed.get());

        try {
            producer2.send(session.createMessage());
            fail("Should not be able to send on closed evicted producer");
        } catch (JMSException ex) {
        }

        connection.close();
    }

    @Test
    public void testCreateAnonymousProducerDoesNotEvictCachedExplicitProducer() throws Exception {
        cf.setUseAnonymousProducers(false);
        cf.setExplicitProducerCacheSize(1);

        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Queue queue = session.createTemporaryQueue();

        JmsPoolMessageProducer producer1 = (JmsPoolMessageProducer) session.createProducer(queue);
        JmsPoolMessageProducer producer2 = (JmsPoolMessageProducer) session.createProducer(queue);

        assertSame(producer1.getProviderMessageProducer(), producer2.getProviderMessageProducer());

        // Creating an anonymous producer should not use the same producer and on next create of
        // explicit producer we should get the cached version.
        JmsPoolMessageProducer producer3 = (JmsPoolMessageProducer) session.createProducer(null);

        assertNotSame(producer1.getProviderMessageProducer(), producer3.getProviderMessageProducer());
        assertNotSame(producer2.getProviderMessageProducer(), producer3.getProviderMessageProducer());

        JmsPoolMessageProducer producer4 = (JmsPoolMessageProducer) session.createProducer(queue);

        assertSame(producer1.getProviderMessageProducer(), producer4.getProviderMessageProducer());
        assertSame(producer2.getProviderMessageProducer(), producer4.getProviderMessageProducer());

        connection.close();
    }

    @Test
    public void testEvictedProducerClosedOnSessionClose() throws Exception {
        cf.setUseAnonymousProducers(false);
        cf.setExplicitProducerCacheSize(1);

        final AtomicBoolean producer1Closed = new AtomicBoolean(false);

        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Queue queue1 = session.createTemporaryQueue();
        Queue queue2 = session.createTemporaryQueue();

        // Producer 1 would enter the cache, and then be evicted by producer 2
        final JmsPoolMessageProducer producer1 = (JmsPoolMessageProducer) session.createProducer(queue1);
        final MockJMSMessageProducer mockProducer1 = (MockJMSMessageProducer) producer1.getProviderMessageProducer();
        final JmsPoolMessageProducer producer2 = (JmsPoolMessageProducer) session.createProducer(queue2);
        final MockJMSMessageProducer mockProducer2 = (MockJMSMessageProducer) producer2.getProviderMessageProducer();

        assertNotSame(producer1.getProviderMessageProducer(), producer2.getProviderMessageProducer());
        assertNotSame(mockProducer1, mockProducer2);

        MockJMSConnection mockConnection = (MockJMSConnection) connection.getProviderConnection();
        mockConnection.addConnectionListener(new MockJMSDefaultConnectionListener() {

            @Override
            public void onCloseMessageProducer(MockJMSSession session, MockJMSMessageProducer producer) throws JMSException {
                if (producer.equals(mockProducer1)) {
                    producer1Closed.set(true);
                }
            }
        });

        session.close();

        assertTrue(producer1Closed.get());

        connection.close();
    }

    @Test
    public void testLeastRecentlyUsedCachedProducerIsEvicted() throws Exception {
        cf.setUseAnonymousProducers(false);
        cf.setExplicitProducerCacheSize(2);

        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Queue queue1 = session.createTemporaryQueue();
        Queue queue2 = session.createTemporaryQueue();
        Queue queue3 = session.createTemporaryQueue();

        JmsPoolMessageProducer producer1 = (JmsPoolMessageProducer) session.createProducer(queue1);
        JmsPoolMessageProducer producer2 = (JmsPoolMessageProducer) session.createProducer(queue2);

        MessageProducer providerProducer1 = producer1.getProviderMessageProducer();
        MessageProducer providerProducer2 = producer2.getProviderMessageProducer();

        // Asking for Queue #1 again makes it the most recently used cached producer so the
        // next producer created for a new destination should push out Queue #2 instead.
        JmsPoolMessageProducer producer1Again = (JmsPoolMessageProducer) session.createProducer(queue1);

        assertSame(providerProducer1, producer1Again.getProviderMessageProducer());

        session.createProducer(queue3);

        JmsPoolMessageProducer producer1Cached = (JmsPoolMessageProducer) session.createProducer(queue1);
        JmsPoolMessageProducer producer2Replaced = (JmsPoolMessageProducer) session.createProducer(queue2);

        // Queue #1 was used most recently and must have been kept, Queue #2 should have been
        // discarded to make room for the Queue #3 producer.
        assertSame(providerProducer1, producer1Cached.getProviderMessageProducer());
        assertNotSame(providerProducer2, producer2Replaced.getProviderMessageProducer());

        connection.close();
    }

    @Test
    public void testCreateSessionAfterSessionWithCachedProducerIsDiscarded() throws Exception {
        cf.setUseAnonymousProducers(false);
        cf.setExplicitProducerCacheSize(2);

        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        JmsPoolSession session = (JmsPoolSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createTemporaryQueue();

        session.createProducer(queue);

        MockJMSSession mockSession = (MockJMSSession) session.getProviderSession();

        // The session returns to the pool holding its cached producer, closing the provider
        // session then means the pool discards it and its cached producer on the next request.
        session.close();
        mockSession.close();

        Session nextSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        assertNotNull(nextSession, "Should be able to create a session after a pooled session was discarded");

        nextSession.close();
        connection.close();
    }

    @Test
    public void testEvictionOfSeeminglyClosedSession() throws Exception {
        cf.setConnectionIdleTimeout(10);
        cf.setMaxSessionsPerConnection(2);
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();

        JmsPoolSession session1 = (JmsPoolSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MockJMSSession mockSession1 = (MockJMSSession) session1.getProviderSession();

        JmsPoolSession session2 = (JmsPoolSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MockJMSSession mockSession2 = (MockJMSSession) session2.getProviderSession();

        assertNotSame(mockSession1, mockSession2);

        session1.close();
        mockSession1.close();

        JmsPoolSession session3 = (JmsPoolSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MockJMSSession mockSession3 = (MockJMSSession) session3.getProviderSession();

        assertNotSame(mockSession1, mockSession3);
        assertNotSame(mockSession2, mockSession3);
    }

    @Test
    public void testProducerGetDeliveryThrowWhenWrapperBuildingCleansUpCreatedResource() throws Exception {
        cf.setUseAnonymousProducers(false);

        final AtomicBoolean producer1Closed = new AtomicBoolean(false);
        final JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        final MockJMSConnection mockConnection = (MockJMSConnection) connection.getProviderConnection();

        mockConnection.addConnectionListener(new MockJMSDefaultConnectionListener() {

            @Override
            public void onCloseMessageProducer(MockJMSSession session, MockJMSMessageProducer producer) throws JMSException {
                producer1Closed.set(true);
            }

            @Override
            public void onMessageProducerGetDeliveryDelay(MockJMSSession session, MockJMSMessageProducer producer) throws JMSException {
                throw new JMSException("Forced error on call to get delivery delay");
            }
        });

        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final Queue queue = session.createTemporaryQueue();

        try {
            session.createProducer(queue);
            fail("Should have thrown from producer create");
        } catch (JMSException ex) {
            // Expected
        }

        assertTrue(producer1Closed.get()); // Created producer should be cleaned up

        session.close();

        connection.close();
    }

    @Test
    public void testPublisherGetDeliveryThrowWhenWrapperBuildingCleansUpCreatedResource() throws Exception {
        cf.setUseAnonymousProducers(false);

        final AtomicBoolean producer1Closed = new AtomicBoolean(false);
        final JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        final MockJMSConnection mockConnection = (MockJMSConnection) connection.getProviderConnection();

        mockConnection.addConnectionListener(new MockJMSDefaultConnectionListener() {

            @Override
            public void onCloseMessageProducer(MockJMSSession session, MockJMSMessageProducer producer) throws JMSException {
                producer1Closed.set(true);
            }

            @Override
            public void onMessageProducerGetDeliveryDelay(MockJMSSession session, MockJMSMessageProducer producer) throws JMSException {
                throw new JMSException("Forced error on call to get delivery delay");
            }
        });

        final TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        final Topic topic = session.createTemporaryTopic();

        try {
            session.createPublisher(topic);
            fail("Should have thrown from producer create");
        } catch (JMSException ex) {
            // Expected
        }

        assertTrue(producer1Closed.get()); // Created producer should be cleaned up

        session.close();

        connection.close();
    }

    @Test
    public void testSenderGetDeliveryThrowWhenWrapperBuildingCleansUpCreatedResource() throws Exception {
        cf.setUseAnonymousProducers(false);

        final AtomicBoolean producer1Closed = new AtomicBoolean(false);
        final JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        final MockJMSConnection mockConnection = (MockJMSConnection) connection.getProviderConnection();

        mockConnection.addConnectionListener(new MockJMSDefaultConnectionListener() {

            @Override
            public void onCloseMessageProducer(MockJMSSession session, MockJMSMessageProducer producer) throws JMSException {
                producer1Closed.set(true);
            }

            @Override
            public void onMessageProducerGetDeliveryDelay(MockJMSSession session, MockJMSMessageProducer producer) throws JMSException {
                throw new JMSException("Forced error on call to get delivery delay");
            }
        });

        final QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        final Queue queue = session.createTemporaryQueue();

        try {
            session.createSender(queue);
            fail("Should have thrown from producer create");
        } catch (JMSException ex) {
            // Expected
        }

        assertTrue(producer1Closed.get()); // Created producer should be cleaned up

        session.close();

        connection.close();
    }

    @Test
    public void testCloseOfSharedAnonymousProducerAfterDelegateClosedRemotely() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createTemporaryQueue();

        // Both wrappers share the single anonymous producer, only the first one created it
        JmsPoolMessageProducer producer1 = (JmsPoolMessageProducer) session.createProducer(queue);
        JmsPoolMessageProducer producer2 = (JmsPoolMessageProducer) session.createProducer(queue);

        MessageProducer providerProducer1 = producer1.getProviderMessageProducer();

        assertSame(providerProducer1, producer2.getProviderMessageProducer());

        // Simulate the remote side closing the producer (e.g. on failover)
        providerProducer1.close();

        producer2.close();
        producer1.close();

        // The closed anonymous producer must have been evicted so a new one is created
        JmsPoolMessageProducer producer3 = (JmsPoolMessageProducer) session.createProducer(queue);

        assertNotSame(providerProducer1, producer3.getProviderMessageProducer());

        producer3.send(session.createMessage());

        connection.close();
    }

    @Test
    public void testCloseOfSharedAnonymousPublisherAfterDelegateClosedRemotely() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTemporaryTopic();

        JmsPoolTopicPublisher publisher1 = (JmsPoolTopicPublisher) session.createPublisher(topic);
        JmsPoolTopicPublisher publisher2 = (JmsPoolTopicPublisher) session.createPublisher(topic);

        MessageProducer providerProducer1 = publisher1.getProviderMessageProducer();

        assertSame(providerProducer1, publisher2.getProviderMessageProducer());

        providerProducer1.close();

        publisher2.close();
        publisher1.close();

        JmsPoolTopicPublisher publisher3 = (JmsPoolTopicPublisher) session.createPublisher(topic);

        assertNotSame(providerProducer1, publisher3.getProviderMessageProducer());

        publisher3.publish(session.createMessage());

        connection.close();
    }

    @Test
    public void testCloseOfSharedAnonymousSenderAfterDelegateClosedRemotely() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createTemporaryQueue();

        JmsPoolQueueSender sender1 = (JmsPoolQueueSender) session.createSender(queue);
        JmsPoolQueueSender sender2 = (JmsPoolQueueSender) session.createSender(queue);

        MessageProducer providerProducer1 = sender1.getProviderMessageProducer();

        assertSame(providerProducer1, sender2.getProviderMessageProducer());

        providerProducer1.close();

        sender2.close();
        sender1.close();

        JmsPoolQueueSender sender3 = (JmsPoolQueueSender) session.createSender(queue);

        assertNotSame(providerProducer1, sender3.getProviderMessageProducer());

        sender3.send(session.createMessage());

        connection.close();
    }

    @Test
    public void testSessionRunInvokesPooledSessionRun() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // Mock session throws as it doesn't not pretend to support this
        assertThrows(JMSRuntimeException.class, () -> session.run());

        session.close();

        assertThrows(JMSRuntimeException.class, () -> session.run());
    }

    @Test
    public void testCloseOfSharedAnonymousProducerAfterSessionIsDestroyed() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createTemporaryQueue();

        JmsPoolMessageProducer producer1 = (JmsPoolMessageProducer) session.createProducer(queue);
        JmsPoolMessageProducer producer2 = (JmsPoolMessageProducer) session.createProducer(queue);

        // Both wrappers share the single destination free producer of the pooled session
        assertSame(producer1.getProviderMessageProducer(), producer2.getProviderMessageProducer());

        // Drops the pooled session that owns the shared producer while the client wrappers are
        // still open, from here on the session has no record of that producer instance.
        cf.clear();

        try {
            producer1.close();
            producer2.close();
        } catch (Exception ex) {
            fail("Shared producer should close cleanly once its session is gone: " + ex);
        }

        session.close();
        connection.close();
    }

    @Test
    public void testCloseOfCachedAndAnonymousProducersAfterSessionIsDestroyed() throws Exception {
        cf.setUseAnonymousProducers(false);
        cf.setExplicitProducerCacheSize(2);

        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createTemporaryQueue();

        JmsPoolMessageProducer cachedProducer = (JmsPoolMessageProducer) session.createProducer(queue);
        JmsPoolMessageProducer anonymousProducer = (JmsPoolMessageProducer) session.createProducer(null);

        assertNotSame(cachedProducer.getProviderMessageProducer(), anonymousProducer.getProviderMessageProducer());

        // Drops the pooled session and the producer cache it holds while the client wrappers are
        // still open, the destination free producer is not cached and is no longer tracked here.
        cf.clear();

        try {
            anonymousProducer.close();
            cachedProducer.close();
        } catch (Exception ex) {
            fail("Producer should close cleanly once its session is gone: " + ex);
        }

        session.close();
        connection.close();
    }

    @Test
    public void testExplicitProducersAreClosedWhenProducerCachingIsDisabled() throws Exception {
        final int producerCreates = 5;

        cf.setUseAnonymousProducers(false);
        cf.setExplicitProducerCacheSize(0);

        assertFalse(cf.isUseAnonymousProducers());
        assertEquals(0, cf.getExplicitProducerCacheSize());

        final AtomicInteger producersCreated = new AtomicInteger();
        final AtomicInteger producersClosed = new AtomicInteger();

        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        MockJMSConnection mockConnection = (MockJMSConnection) connection.getProviderConnection();
        mockConnection.addConnectionListener(new MockJMSDefaultConnectionListener() {

            @Override
            public void onCreateMessageProducer(MockJMSSession session, MockJMSMessageProducer producer) throws JMSException {
                producersCreated.incrementAndGet();
            }

            @Override
            public void onCloseMessageProducer(MockJMSSession session, MockJMSMessageProducer producer) throws JMSException {
                producersClosed.incrementAndGet();
            }
        });

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createTemporaryQueue();

        // With no cache configured every create must make a new provider producer and nothing
        // else holds that producer, so closing the wrapper must release the provider resource
        // and not leave it open for the life of the pooled session instance.
        for (int i = 0; i < producerCreates; i++) {
            MessageProducer producer = session.createProducer(queue);
            producer.close();
        }

        assertEquals(producerCreates, producersCreated.get());
        assertEquals(producerCreates, producersClosed.get());

        session.close();
        connection.close();
    }

    @Test
    public void testIdleAllMustNotIdleMoreSessionsThanTheMaxIdleLimit() throws Exception {
        final int NUM_SESSIONS = 3;
        final int NUM_MAX_IDLE_SESSIONS = 1;

        cf.setMaxSessionsPerConnection(NUM_SESSIONS);
        cf.setMaxIdleSessionsPerConnection(NUM_MAX_IDLE_SESSIONS);
        cf.setBlockIfSessionPoolIsFull(false);

        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        JmsAbstractSessionPool<?> sessionPool = (JmsAbstractSessionPool<?>) connection.getStatistics();

        assertEquals(NUM_SESSIONS, sessionPool.getMaxSessions());
        assertEquals(NUM_MAX_IDLE_SESSIONS, sessionPool.getMaxIdleSessions());

        for (int i = 0; i < NUM_SESSIONS; i++) {
            connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        }

        assertEquals(NUM_SESSIONS, connection.getNumActiveSessions());
        assertEquals(0, connection.getNumIdleSessions());

        sessionPool.idle();

        // Force idling must respect the configured idle limit and close the excess sessions
        // rather than pushing every active proxy into the idle queue.
        assertEquals(NUM_MAX_IDLE_SESSIONS, connection.getNumIdleSessions(), "idle queue overshot maxIdleSessionsPerConnection");
        assertEquals(NUM_MAX_IDLE_SESSIONS, connection.getNumSessions(), "surplus sessions should have been closed on force idle");

        connection.close();
    }

    @Test
    public void testReturnedSessionIsClosedWhenIdleQueueIsOverTheMaxIdleLimit() throws Exception {
        final int NUM_SESSIONS = 3;
        final int NUM_MAX_IDLE_SESSIONS = 1;

        cf.setMaxSessionsPerConnection(NUM_SESSIONS);
        cf.setMaxIdleSessionsPerConnection(NUM_MAX_IDLE_SESSIONS);
        cf.setBlockIfSessionPoolIsFull(false);

        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        JmsAbstractSessionPool<?> sessionPool = (JmsAbstractSessionPool<?>) connection.getStatistics();

        for (int i = 0; i < NUM_SESSIONS; i++) {
            connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        }

        // Overfill the idle queue the same way the force idle path does today. Once the queue
        // is past the limit the release check must still close returned sessions. If the force
        // idle is fixed to honour the limit this scenario has to be reached another way.
        sessionPool.idle();
        assumeTrue(connection.getNumIdleSessions() > NUM_MAX_IDLE_SESSIONS,
            "force idle no longer overfills the idle queue, this case needs a different setup");

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertEquals(NUM_SESSIONS - 1, connection.getNumIdleSessions());

        session.close();

        // The idle queue is over its limit so the returned session must be discarded, not
        // added back to the idle queue and held for the life of the pooled connection.
        assertEquals(NUM_SESSIONS - 1, connection.getNumIdleSessions(), "returned session should have been closed, idle queue is already over the limit");
        assertEquals(NUM_SESSIONS - 1, connection.getNumSessions());

        connection.close();
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testIdleAllWakesCallerBlockedOnSessionPool() throws Exception {
        cf.setMaxSessionsPerConnection(1);
        cf.setBlockIfSessionPoolIsFull(true);
        cf.setBlockIfSessionPoolIsFullTimeout(0);

        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();
        JmsAbstractSessionPool<?> sessionPool = (JmsAbstractSessionPool<?>) connection.getStatistics();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertEquals(1, connection.getNumActiveSessions());

        final CountDownLatch finished = new CountDownLatch(1);
        final AtomicReference<Throwable> waiterError = new AtomicReference<>();

        Thread waiter = new Thread(() -> {
            try {
                connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            } catch (Throwable error) {
                waiterError.set(error);
            } finally {
                finished.countDown();
            }
        }, "Pooled-JMS-Session-Pool-Waiter");
        waiter.setDaemon(true);
        waiter.start();

        // Let the second caller park in the session pool waiting for a free session slot.
        long deadline = System.currentTimeMillis() + 5_000;
        while (waiter.getState() != Thread.State.WAITING && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }

        assertEquals(Thread.State.WAITING, waiter.getState(), "caller should be blocked waiting for a pooled session");

        // Force idling frees the active session slot, waiters must be signalled so they can
        // take it instead of staying parked on a full pool that is no longer full.
        sessionPool.idle();
        assertEquals(0, connection.getNumActiveSessions());

        assertTrue(finished.await(5, TimeUnit.SECONDS), "blocked caller was never signalled when the session pool was force idled");
        waiter.join(TimeUnit.SECONDS.toMillis(5));
        assertNull(waiterError.get(), "signalled caller should have been handed a session");

        session.close();
        connection.close();
    }

    @Test
    public void testProviderSessionCloseFailureIsNotReportedAndSessionIsNeverReused() throws Exception {
        cf.setMaxSessionsPerConnection(4);
        cf.setMaxIdleSessionsPerConnection(1);
        cf.setBlockIfSessionPoolIsFull(false);

        JmsPoolConnection connection = (JmsPoolConnection) cf.createConnection();

        final List<MockJMSSession> closeAttempts = new ArrayList<>();
        MockJMSConnection providerConnection = (MockJMSConnection) connection.getProviderConnection();
        providerConnection.addConnectionListener(new MockJMSDefaultConnectionListener() {

            @Override
            public void onCloseSession(MockJMSSession session) throws JMSException {
                closeAttempts.add(session);
            }
        });

        // The pool only closes a provider session once the idle queue is full, so create more
        // sessions than the idle limit and make every provider session report a close failure.
        for (int i = 0; i < 3; i++) {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            ((MockJMSSession) ((JmsPoolSession) session).getProviderSession())
                .setExceptionToThrowOnClose(new JMSException("Forced provider session close failure"));
        }

        // Session close failures are swallowed by the session proxy (logged at TRACE) so the
        // pooled connection close must still complete and must still release its pooled
        // reference, and the pool must never hand a session it tried to close to a new caller.
        connection.close();

        assertFalse(closeAttempts.isEmpty(), "no provider session close was attempted, this test is not exercising the failure path");

        JmsPoolConnection reopened = (JmsPoolConnection) cf.createConnection();
        for (int i = 0; i < 2; i++) {
            Session session = reopened.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MockJMSSession providerSession = (MockJMSSession) ((JmsPoolSession) session).getProviderSession();
            assertFalse(closeAttempts.contains(providerSession),
                "a provider session whose close failed was handed to a new caller");
            session.close();
        }

        reopened.close();
    }
}
