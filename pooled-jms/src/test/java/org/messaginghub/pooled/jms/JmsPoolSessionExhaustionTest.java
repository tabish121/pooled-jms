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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.messaginghub.pooled.jms.mock.MockJMSConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.ResourceAllocationException;
import jakarta.jms.Session;

@Timeout(60)
public class JmsPoolSessionExhaustionTest extends JmsPoolTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Override
    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);

        factory = new MockJMSConnectionFactory();
        cf = new JmsPoolConnectionFactory();
        cf.setConnectionFactory(factory);
        cf.setMaxConnections(1);
        cf.setBlockIfSessionPoolIsFull(false);
        cf.setMaxSessionsPerConnection(1);
    }

    @Test
    public void testCreateSessionThrowsWhenSessionPoolExhaustedSharedConnectionNoTimeout() throws JMSException {
        doTestCreateSessionThrowsWhenSessionPoolExhaustedSharedConnection(false);
    }

    @Test
    public void testCreateSessionThrowsWhenSessionPoolExhaustedSharedConnectionTimeout() throws JMSException {
        doTestCreateSessionThrowsWhenSessionPoolExhaustedSharedConnection(true);
    }

    private void doTestCreateSessionThrowsWhenSessionPoolExhaustedSharedConnection(boolean timeout) throws JMSException {
        if (timeout) {
            cf.setBlockIfSessionPoolIsFull(true);
            cf.setBlockIfSessionPoolIsFullTimeout(50);
        } else {
            cf.setBlockIfSessionPoolIsFull(false);
        }

        Connection connection1 = cf.createConnection();
        Connection connection2 = cf.createConnection();

        // One Connections should be able to create one session
        Session session1 = connection1.createSession();

        long startTime = System.currentTimeMillis();
        try {
            connection2.createSession();
            fail("Should not be able to create a second Session Connection 2");
        } catch (ResourceAllocationException ex) {
            if (timeout) {
                assertTrue((System.currentTimeMillis() - startTime) > 30);
            }
            LOG.info("Caught exception on session create as expected: {}", ex.getMessage());
        }

        assertNotNull(session1);

        startTime = System.currentTimeMillis();
        try {
            connection1.createSession();
            fail("Should not be able to create a second Session on Connection 1");
        } catch (ResourceAllocationException ex) {
            if (timeout) {
                assertTrue((System.currentTimeMillis() - startTime) > 30);
            }
            LOG.info("Caught exception on session create as expected: {}", ex.getMessage());
        }

        session1.close();

        // Now the other Connections should be able to create one session
        Session session2 = connection2.createSession();

        assertNotNull(session1);
        assertNotNull(session2);
        assertNotSame(session1, session2);
    }

    @Test
    public void testCreateSessionThrowsWhenSessionPoolExhaustedNonSharedConnectionNoTimeout() throws JMSException {
        doTestCreateSessionThrowsWhenSessionPoolExhaustedNonSharedConnection(false);
    }

    @Test
    public void testCreateSessionThrowsWhenSessionPoolExhaustedNonSharedConnectionWithTimeout() throws JMSException {
        doTestCreateSessionThrowsWhenSessionPoolExhaustedNonSharedConnection(true);
    }

    private void doTestCreateSessionThrowsWhenSessionPoolExhaustedNonSharedConnection(boolean timeout) throws JMSException {

        cf.setMaxConnections(2);
        if (timeout) {
            cf.setBlockIfSessionPoolIsFull(true);
            cf.setBlockIfSessionPoolIsFullTimeout(50);

            assertEquals(50, cf.getBlockIfSessionPoolIsFullTimeout());
        } else {
            cf.setBlockIfSessionPoolIsFull(false);
        }

        Connection connection1 = cf.createConnection();
        Connection connection2 = cf.createConnection();

        // Both Connections should be able to create one session
        Session session1 = connection1.createSession();
        Session session2 = connection2.createSession();

        assertNotNull(session1);
        assertNotNull(session2);
        assertNotSame(session1, session2);

        long startTime = System.currentTimeMillis();
        try {
            connection1.createSession();
            fail("Should not be able to create a second Session on Connection 1");
        } catch (ResourceAllocationException ex) {
            if (timeout) {
                assertTrue((System.currentTimeMillis() - startTime) > 30);
            }
            LOG.info("Caught exception on session create as expected: {}", ex.getMessage());
        }

        startTime = System.currentTimeMillis();
        try {
            connection2.createSession();
            fail("Should not be able to create a second Session Connection 2");
        } catch (ResourceAllocationException ex) {
            if (timeout) {
                assertTrue((System.currentTimeMillis() - startTime) > 30);
            }
            LOG.info("Caught exception on session create as expected: {}", ex.getMessage());
        }

        session1.close();
        session2.close();

        // Both Connections should be able to create one session
        Session session3 = connection1.createSession();
        Session session4 = connection2.createSession();

        assertNotNull(session3);
        assertNotNull(session4);
        assertNotSame(session1, session3);
        assertNotSame(session2, session4);
        assertNotSame(session3, session4);
    }

    @Test
    public void testCreateSessionsUnlimitedWhenSessionLimitDisabled() throws Exception {
        cf.setMaxSessionsPerConnection(-10);
        assertEquals(-1, cf.getMaxSessionsPerConnection(), "Negative limits should disable the session limit");

        Connection connection = cf.createConnection();

        // Create more sessions than the default limit to verify it is fully disabled.
        final Session[] sessions = new Session[510];
        for (int i = 0; i < sessions.length; i++) {
            sessions[i] = connection.createSession();
            assertNotNull(sessions[i]);
        }
    }

    @Test
    public void testBlockForeverWaiterStaysBlockedOnUnrelatedSessionClose() throws Exception {
        doTestWaiterStaysBlockedOnUnrelatedSessionClose(false);
    }

    @Test
    public void testWaiterWithTimeoutStaysBlockedOnUnrelatedSessionClose() throws Exception {
        doTestWaiterStaysBlockedOnUnrelatedSessionClose(true);
    }

    /**
     * Session limits are applied per acknowledgement mode so the close of a session of a
     * different type can wake a blocked createSession call without being able to satisfy
     * it. The waiter must not treat that wake up as a timeout or hand out a new session
     * beyond the configured limit and should only complete once a matching session is
     * released.
     */
    private void doTestWaiterStaysBlockedOnUnrelatedSessionClose(boolean withTimeout) throws Exception {
        cf.setBlockIfSessionPoolIsFull(true);
        if (withTimeout) {
            cf.setBlockIfSessionPoolIsFullTimeout(10_000);
        }

        Connection connection = cf.createConnection();
        Session held = connection.createSession();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Session> blocked = executor.submit(() -> connection.createSession());

            // Allow the taker to enter the blocked state then wake it with the close of
            // a session from a different acknowledgement mode which cannot free a slot.
            Thread.sleep(500);
            connection.createSession(false, Session.CLIENT_ACKNOWLEDGE).close();

            assertThrows(TimeoutException.class, () -> blocked.get(500, TimeUnit.MILLISECONDS),
                "Session taker should remain blocked when no matching session was released");

            held.close();

            assertNotNull(blocked.get(5, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
        }
    }
}
