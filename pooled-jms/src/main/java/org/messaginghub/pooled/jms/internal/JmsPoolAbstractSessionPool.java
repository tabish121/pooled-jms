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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import jakarta.jms.IllegalStateException;
import jakarta.jms.JMSException;

/**
 * Base for the session pool used to manage loaned and free sessions
 */
public abstract class JmsPoolAbstractSessionPool<SP extends JmsPoolAbstractSessionProxy<?>> {

    // TODO: Test on borrow
    // TODO: Test on return and cleanup
    // TODO: Open and Close as needed on borrow and return

    private final Map<JmsPoolSessionKey, JmsPoolKeyedSessionsPool> pools = new HashMap<>();

    private final int maxSessions;
    private final int maxIdleSessions;
    private final boolean blockIfLimitReached;
    private final long blockIfLimitReachedTimeout;

    private int numSessions;
    private int numIdleSessions;
    private int numActiveSessions;

    JmsPoolAbstractSessionPool(JmsPoolConnectionConfiguration configuration) {
        this.maxSessions = configuration.getMaxSessionsPerConnection();
        this.maxIdleSessions = configuration.getMaxIdleSessionsPerConnection();
        this.blockIfLimitReached = configuration.isBlockIfSessionPoolIsFull();
        this.blockIfLimitReachedTimeout = configuration.getBlockIfSessionPoolIsFullTimeout();
    }

    /**
     * {@return the max number of sessions the session pool will allocate}
     */
    public int getMaxSessions() {
        return maxSessions;
    }

    /**
     * {@return the max number of idle sessions the session pool will allow before closing older idle sessions}
     */
    public int getMaxIdleSessions() {
        return maxIdleSessions;
    }

    /**
     * {@return if the pool will block if no idle sessions exist and the active sessions is at the limit}
     */
    public boolean isBlockIfLimitReached() {
        return blockIfLimitReached;
    }

    /**
     * {@return the time in milliseconds to wait if block on session limit reached}
     */
    public long getBlockIfLimitReachedTimeout() {
        return blockIfLimitReachedTimeout;
    }

    /**
     * {@return the total number of sessions that are currently idle and active in this session pool}
     */
    public synchronized int getSessionCount() {
        return numSessions;
    }

    /**
     * {@return the number of sessions that are currently idle in this session pool}
     */
    public synchronized int getIdleSessionCount() {
        return numIdleSessions;
    }

    /**
     * {@return the number of sessions that are currently loaned out from this session pool}
     */
    public synchronized int getActiveSessionCount() {
        return numActiveSessions;
    }

    /**
     * Returns the next available session or creates a new one to add to the pool of sessions.
     * <p>
     * Sessions are shared individually as requested and are created on demand up to the session
     * limit and added into the pool. If the maximum number of sessions is reached then either the
     * method will block for the configured amount of time or will return <code>null</code> immediately
     * to indicate no sessions are available.
     *
     * @param transacted
     * 		If the session is to be configured for transactional mode
     * @param sessionMode
     * 		The assigned session mode of the session to be returned
     *
     * @return the next available session from the pool, or <code>null</code> if already at maximum.
     *
     * @throws JMSException if a new session cannot be created to match the demand
     */
    public synchronized SP nextSession(boolean transacted, int sessionMode) throws JMSException {
        return pools.computeIfAbsent(new JmsPoolSessionKey(transacted, sessionMode), key -> new JmsPoolKeyedSessionsPool()).nextIdle();
    }

    /**
     * Closes the pool returning all sessions into the internal free list for use when the
     * session pool is used again by a newly loaned connection.
     *
     * TODO: Need an open or this could just be a single action to quiesce
     */
    public synchronized void close() {
        pools.forEach((k, v) -> v.close());
    }

    /**
     * Terminal operation that closes and destroys all pooled sessions and cannot be undone.
     */
    public synchronized void destroy() {
        pools.forEach((k, v) -> v.destroy());
    }

    protected boolean validateSession(SP sessioProxy) {
        try {
            sessioProxy.getProviderSession().getTransacted();
        } catch (IllegalStateException jmsISE) {
            return false;
        } catch (Exception ambiguous) {
            // Unsure if session is still valid so continue as if it still is.
        }

        return true;
    }

    protected void activateSession(SP sessioProxy) {
        sessioProxy.open();
    }

    protected void destroySession(SP sessionProxy) {
        sessionProxy.internalClose();
    }

    protected abstract SP createSessionProxy(boolean transacted,
                                             int sessionMode,
                                             Consumer<SP> onSessionClosed,
                                             Consumer<SP> onSessionDestroyed) throws JMSException;

    private class JmsPoolKeyedSessionsPool {

        private List<SP> activeSessions;
        private List<SP> idleSessions;

        private int numSessions;

        public void close() {
            activeSessions.forEach(session -> {
                session.close();
            });
        }

        public void destroy() {
            activeSessions.forEach(session -> {
                session.destroy();
            });
            idleSessions.forEach(session -> {
                session.destroy();
            });
        }

        public SP nextIdle() throws JMSException {
            return null;
        }

        protected void onSessionClosed(SP session) {
            synchronized (JmsPoolAbstractSessionPool.this) {
                // TODO
            }
        }

        protected void onSessionDestroyed(SP session) {
            synchronized (JmsPoolAbstractSessionPool.this) {
                // TODO
            }
        }
    }
}
