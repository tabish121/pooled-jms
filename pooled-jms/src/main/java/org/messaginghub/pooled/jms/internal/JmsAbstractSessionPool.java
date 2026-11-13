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

import java.lang.invoke.MethodHandles;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

import org.messaginghub.pooled.jms.JmsPoolConnectionStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.jms.IllegalStateException;
import jakarta.jms.JMSException;
import jakarta.jms.ResourceAllocationException;

/**
 * Base for the session pool used to manage loaned and free sessions
 *
 * @param <SP> The type of the session proxy that is pooled in this instance
 */
public abstract class JmsAbstractSessionPool<SP extends JmsAbstractSessionProxy<SP>> implements JmsPoolConnectionStatistics {

    private record JmsSessionKey(boolean transacted, int acknowledgementMode) {}

    /**
     * Allocation of arrays will generally fail as OOM beyond this point so we cap it as an
     * attempt at a safe upper bound to sanity check user configured bounds.
     */
    private static final int SAFE_MAX_SESSION_UPPER_BOUND = Integer.MAX_VALUE - 8;

    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final Map<JmsSessionKey, JmsKeyedSessionPool> pools = new HashMap<>();
    private final JmsConnectionProxyConfiguration configuration;

    private final int maxActiveSessions;
    private final int maxIdleSessions;

    private boolean destroyed;

    /**
     * Creates an instance of the session pool with the provided immutable configuration
     *
     * @param configuration
     * 		Immutable configuration that should be used by this session pool
     */
    JmsAbstractSessionPool(JmsConnectionProxyConfiguration configuration) {
        this.configuration = configuration;

        if (configuration.getMaxSessionsPerConnection() < 0) {
            this.maxActiveSessions = SAFE_MAX_SESSION_UPPER_BOUND;
        } else {
            this.maxActiveSessions = Math.min(configuration.getMaxSessionsPerConnection(), SAFE_MAX_SESSION_UPPER_BOUND);
        }

        this.maxIdleSessions = Math.min(maxActiveSessions, configuration.getMaxIdleSessionsPerConnection());
    }

    /**
     * {@return the max number of sessions the session pool will allocate}
     */
    public int getMaxSessions() {
        return maxActiveSessions;
    }

    /**
     * {@return the max number of idle sessions the session pool will allow before closing older idle sessions}
     */
    public int getMaxIdleSessions() {
        return maxIdleSessions;
    }

    @Override
    public synchronized int getSessionCount() {
        final AtomicInteger count = new AtomicInteger();

        pools.values().forEach(pool -> count.addAndGet(pool.getNumSessions()));

        return count.get();
    }

    @Override
    public synchronized int getActiveSessionCount() {
        final AtomicInteger count = new AtomicInteger();

        pools.values().forEach(pool -> count.addAndGet(pool.getNumActive()));

        return count.get();
    }

    @Override
    public synchronized int getIdleSessionCount() {
        final AtomicInteger count = new AtomicInteger();

        pools.values().forEach(pool -> count.addAndGet(pool.getNumIdle()));

        return count.get();
    }

    @Override
    public synchronized int getSessionCount(boolean transacted, int acknowledgementMode) {
        final JmsSessionKey key = new JmsSessionKey(transacted, acknowledgementMode);
        final JmsKeyedSessionPool pool = pools.get(key);

        if (pool != null) {
            return pool.getNumSessions();
        } else {
            return 0;
        }
    }

    @Override
    public synchronized int getActiveSessionCount(boolean transacted, int acknowledgementMode) {
        final JmsSessionKey key = new JmsSessionKey(transacted, acknowledgementMode);
        final JmsKeyedSessionPool pool = pools.get(key);

        if (pool != null) {
            return pool.getNumActive();
        } else {
            return 0;
        }
    }

    @Override
    public synchronized int getIdleSessionCount(boolean transacted, int acknowledgementMode) {
        final JmsSessionKey key = new JmsSessionKey(transacted, acknowledgementMode);
        final JmsKeyedSessionPool pool = pools.get(key);

        if (pool != null) {
            return pool.getNumIdle();
        } else {
            return 0;
        }
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
        return pools.computeIfAbsent(new JmsSessionKey(transacted, sessionMode), key -> new JmsKeyedSessionPool(key)).nextIdle();
    }

    /**
     * Idles the pool returning all sessions into the internal free list for use when the
     * session pool is used again by a newly loaned connection.
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

    /**
     * The concrete session pool implementation should create a new session proxy instance that wraps
     * a session from the provider connection.
     *
     * @param transacted
     * 		Boolean indicating if the session instance being created is a transacted session
     * @param sessionMode
     * 		The assigned session mode the session instance is being created with
     * @param manager
     * 		The session proxy manager that owns the new session proxy instance once created
     *
     * @return the newly created session proxy instance
     *
     * @throws JMSException if an error occurs during creation of the provider session
     */
    protected abstract SP createSessionProxy(boolean transacted,
                                             int sessionMode,
                                             JmsSessionProxyManager<SP> manager) throws JMSException;

    private void checkedIsDestroyed() throws IllegalStateException {
        if (destroyed) {
            throw new IllegalStateException("The JMS Session pool for this connection has been destroyed");
        }
    }

    // Expected to operate under the lock of the parent session pool
    private class JmsKeyedSessionPool implements JmsSessionProxyManager<SP> {

        private final JmsSessionKey sessionKey;

        private final Queue<SP> activeSessions;
        private final Queue<SP> idleSessions;

        JmsKeyedSessionPool(JmsSessionKey sessionKey) {
            this.sessionKey = sessionKey;
            this.activeSessions = new ArrayDeque<SP>();
            this.idleSessions = new ArrayDeque<SP>();
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
            idleSessions.addAll(activeSessions);
            activeSessions.clear();
        }

        public void destroy() {
            activeSessions.forEach(session -> {
                try {
                    session.internalClose();
                } catch (Exception e) {}
            });
            idleSessions.forEach(session -> {
                try {
                    session.internalClose();
                } catch (Exception e) {}
            });

            activeSessions.clear();
            idleSessions.clear();

            // Signal any waiters to wake up and see that the pool is now closed.
            JmsAbstractSessionPool.this.notifyAll();
        }

        public SP nextIdle() throws JMSException {
            final long deadline;

            long timeout = 0;

            if (configuration.isBlockIfSessionPoolIsFull() && configuration.getBlockIfSessionPoolIsFullTimeout() > 0) {
                deadline = System.currentTimeMillis() + configuration.getBlockIfSessionPoolIsFullTimeout();
                timeout = configuration.getBlockIfSessionPoolIsFullTimeout();
            } else {
                deadline = 0;
            }

            while (!destroyed) {
                if (activeSessions.size() >= getMaxSessions()) {
                    if (!configuration.isBlockIfSessionPoolIsFull()) {
                        throw new ResourceAllocationException(
                            "JMS Pool has reached the max number of pooled sessions for this connection.");
                    }

                    try {
                        if (configuration.getBlockIfSessionPoolIsFullTimeout() > 0) {
                            JmsAbstractSessionPool.this.wait(timeout);
                        } else {
                            JmsAbstractSessionPool.this.wait();
                        }
                    } catch (InterruptedException e) {
                        // Choosing to throw resource allocation error here as clients are more likely
                        // to handle this gracefully than other types which are less clear about the
                        // outcome from a JMS perspective. Set the interrupted state so that an outside
                        // observer can witness the state.
                        Thread.currentThread().interrupt();

                        throw new ResourceAllocationException(
                            "JMS Pool has reached the max number of pooled sessions for this connection and " +
                            "was interrupted while waiting for one to become available.");
                    } finally {
                        checkedIsDestroyed();
                    }

                    if (configuration.getBlockIfSessionPoolIsFullTimeout() > 0) {
                        timeout = deadline - System.currentTimeMillis();

                        if (activeSessions.size() == getMaxSessions() && timeout <= 0) {
                            throw new ResourceAllocationException(
                                "JMS Pool timed out waiting for an idle session to become available.");
                        }
                    }

                    continue; // Re-check the limits and wait more if needed
                }

                final SP session;

                if (idleSessions.isEmpty()) {
                    session = createSessionProxy(sessionKey.transacted(), sessionKey.acknowledgementMode(), this);
                } else {
                    session = idleSessions.poll();
                }

                if (session.validateSession()) {
                    activeSessions.add(session);
                    return session;
                } else {
                    session.internalClose();
                }
            }

            checkedIsDestroyed();

            throw new ResourceAllocationException("Unknown Error: Could not provide a JMS session from the pool.");
        }

        /*
         * Below calls are made from the pooled session back into its manager and are required
         * to lock the parent session pool instance to be thread safe against calls made to the
         * overall session pool.
         */

        @Override
        public SP releaseSession(SP session) {
            boolean closeSession = false;

            synchronized (JmsAbstractSessionPool.this) {
                // Check the returned session to try and detect closed while being
                // returned and also check that the idle sessions queue isn't full.
                // If either of those cases occurs we just destroy the session and
                // remove it from the closed sessions pool.
                final boolean wasActive = activeSessions.remove(session);

                if (destroyed || !session.validateSession() || idleSessions.size() == getMaxIdleSessions() || !wasActive) {
                    idleSessions.remove(session);
                    closeSession = true;
                } else {
                    idleSessions.offer(session);
                }

                // Signal waiters to check if a session can be taken now.
                JmsAbstractSessionPool.this.notifyAll();
            }

            if (closeSession) {
                try {
                    session.internalClose();
                } catch (Exception e) {
                    LOG.trace("Ignored error on close of pooled session", e);
                }
            }

            return session;
        }

        @Override
        public SP closeSession(SP session) {
            synchronized (JmsAbstractSessionPool.this) {
                activeSessions.remove(session);
                idleSessions.remove(session);

                // Signal waiters to check if a session can be taken now.
                JmsAbstractSessionPool.this.notifyAll();
            }

            try {
                session.internalClose();
            } catch (Exception e) {
                LOG.trace("Ignored error on close of pooled session", e);
            }

            return session;
        }
    }
}
