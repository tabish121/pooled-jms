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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import jakarta.jms.IllegalStateException;
import jakarta.jms.JMSException;
import jakarta.jms.ResourceAllocationException;

/**
 * Base for the session pool used to manage loaned and free sessions
 */
public abstract class JmsPoolAbstractSessionPool<SP extends JmsPoolAbstractSessionProxy<?>> {

    private final Map<JmsPoolSessionKey, JmsPoolKeyedSessionsPool> pools = new HashMap<>();

    private final JmsPoolConnectionConfiguration configuration;

    private boolean destroyed;

    JmsPoolAbstractSessionPool(JmsPoolConnectionConfiguration configuration) {
        this.configuration = configuration;
    }

    /**
     * {@return the max number of sessions the session pool will allocate}
     */
    public int getMaxSessions() {
        return configuration.getMaxSessionsPerConnection();
    }

    /**
     * {@return the max number of idle sessions the session pool will allow before closing older idle sessions}
     */
    public int getMaxIdleSessions() {
        return configuration.getMaxIdleSessionsPerConnection();
    }

    /**
     * {@return if the pool will block if no idle sessions exist and the active sessions is at the limit}
     */
    public boolean isBlockIfLimitReached() {
        return configuration.isBlockIfSessionPoolIsFull();
    }

    /**
     * {@return the time in milliseconds to wait if block on session limit reached}
     */
    public long getBlockIfLimitReachedTimeout() {
        return configuration.getBlockIfSessionPoolIsFullTimeout();
    }

    /**
     * {@return the total number of sessions that are currently idle and active in this session pool}
     */
    public synchronized int getSessionCount() {
        final AtomicInteger count = new AtomicInteger();

        pools.values().forEach(pool -> count.addAndGet(pool.getNumSessions()));

        return count.get();
    }

    /**
     * {@return the number of sessions that are currently idle in this session pool}
     */
    public synchronized int getIdleSessionCount() {
        final AtomicInteger count = new AtomicInteger();

        pools.values().forEach(pool -> count.addAndGet(pool.getNumIdle()));

        return count.get();
    }

    /**
     * {@return the number of sessions that are currently loaned out from this session pool}
     */
    public synchronized int getActiveSessionCount() {
        final AtomicInteger count = new AtomicInteger();

        pools.values().forEach(pool -> count.addAndGet(pool.getNumActive()));

        return count.get();
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
        return pools.computeIfAbsent(new JmsPoolSessionKey(transacted, sessionMode), key -> new JmsPoolKeyedSessionsPool(key)).nextIdle();
    }

    /**
     * Idles the pool returning all sessions into the internal free list for use when the
     * session pool is used again by a newly loaned connection.
     *
     * @throws JMSException if an error occurs while idling the session pool.
     */
    public synchronized void idle() {
        if (!destroyed) {
            pools.forEach((k, v) -> v.idleAll());
        }
    }

    /**
     * Terminal operation that closes and destroys all pooled sessions and cannot be undone.
     */
    public synchronized void destroy() {
        if (!destroyed) {
            destroyed = true;
            pools.forEach((k, v) -> v.destroy());
            pools.clear();
        }
    }

    private void checkedIsDestroyed() throws IllegalStateException {
        if (destroyed) {
            throw new IllegalStateException("The JMS Session pool for this connection has been destroyed");
        }
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

    // Expected to operate under the lock of the parent session pool
    private class JmsPoolKeyedSessionsPool {

        private static final Queue<?> EMPTY_QUEUE = new ArrayDeque<>(0);

        private final JmsPoolSessionKey sessionKey;

        private Queue<SP> activeSessions;
        private Queue<SP> idleSessions;

        JmsPoolKeyedSessionsPool(JmsPoolSessionKey sessionKey) {
            this.sessionKey = sessionKey;
            this.activeSessions = new ArrayDeque<SP>(getMaxSessions());
            this.idleSessions = new ArrayDeque<SP>(getMaxIdleSessions());
        }

        public int getNumSessions() {
            return idleSessions.size() + activeSessions.size();
        }

        public int getNumIdle() {
            return idleSessions.size();
        }

        public int getNumActive() {
            return activeSessions.size();
        }

        public void idleAll() {
            final List<SP> active = new ArrayList<>(activeSessions);

            active.forEach(session -> {
                session.close();
            });
        }

        @SuppressWarnings("unchecked")
        public void destroy() {
            final Queue<SP> active = activeSessions;
            final Queue<SP> idle = idleSessions;

            activeSessions = (Queue<SP>) EMPTY_QUEUE;
            idleSessions = (Queue<SP>) EMPTY_QUEUE;

            active.forEach(session -> {
                try {
                    session.destroy();
                } catch (Exception e) {}
            });
            idle.forEach(session -> {
                try {
                    session.destroy();
                } catch (Exception e) {}
            });

            active.clear();
            idle.clear();

            // Signal any waiters to wake up and see that the pool is now closed.
            JmsPoolAbstractSessionPool.this.notifyAll();
        }

        public SP nextIdle() throws JMSException {
            long timeout = getBlockIfLimitReachedTimeout();
            long deadline = 0;

            if (getBlockIfLimitReachedTimeout() > 0) {
                deadline = System.currentTimeMillis() + getBlockIfLimitReachedTimeout();
            }

            while (!destroyed) {
                if (activeSessions.size() == getMaxSessions()) {
                    if (isBlockIfLimitReached() && timeout != 0) {
                        try {
                            if (timeout > 0) {
                                JmsPoolAbstractSessionPool.this.wait(getBlockIfLimitReachedTimeout());
                            } else {
                                JmsPoolAbstractSessionPool.this.wait();
                            }

                            timeout = Math.max(deadline - System.currentTimeMillis(), 0);
                        } catch (InterruptedException e) {
                            continue;
                        } finally {
                            checkedIsDestroyed();
                        }

                        continue; // Pinned to check on max active sessions.
                    } else {
                        throw new ResourceAllocationException(
                            "JMS Pool has reached the max number of pooled sessions for this connection.");
                    }
                }

                final SP session;

                if (idleSessions.isEmpty()) {
                    session = createSessionProxy(sessionKey.isTransacted(),
                                                 sessionKey.getAckMode(),
                                                 this::onSessionClosed,
                                                 this::onSessionDestroyed);
                } else {
                    session = idleSessions.poll();
                }

                if (session.validateSession()) {
                    activeSessions.add(session);

                    session.open();

                    return session;
                } else {
                    session.destroy();
                }
            }

            checkedIsDestroyed();

            throw new ResourceAllocationException("Unknown Error: Could not provide a JMS session from the pool.");
        }

        protected void onSessionClosed(SP session) {
            synchronized (JmsPoolAbstractSessionPool.this) {
                // Check the returned session to try and detect closed while being
                // returned and also check that the idle sessions queue isn't full.
                // If either of those cases occurs we just destroy the session and
                // remove it from the closed sessions pool.
                activeSessions.remove(session);

                if (destroyed || !session.validateSession() || idleSessions.size() == getMaxIdleSessions()) {
                    session.destroy();
                } else {
                    idleSessions.offer(session);
                }

                // Signal waiters to check if a session can be taken now.
                JmsPoolAbstractSessionPool.this.notifyAll();
            }
        }

        protected void onSessionDestroyed(SP session) {
            synchronized (JmsPoolAbstractSessionPool.this) {
                activeSessions.remove(session);
                idleSessions.remove(session);

                if (!destroyed) {
                    // Signal waiters to check if a session can be taken now.
                    JmsPoolAbstractSessionPool.this.notifyAll();
                }
            }
        }
    }
}
