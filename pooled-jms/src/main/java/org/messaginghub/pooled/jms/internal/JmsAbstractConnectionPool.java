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
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.messaginghub.pooled.jms.JmsPoolConnectionFactoryStatistics;
import org.messaginghub.pooled.jms.JmsPoolConnectionStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.jms.IllegalStateException;
import jakarta.jms.JMSException;

/**
 * Base for the connection pool used to manage loaned and free connections
 *
 * @param <CP> the actual connection proxy type that will be pooled
 */
public abstract class JmsAbstractConnectionPool<CP extends JmsAbstractConnectionProxy<CP, ?>> implements JmsPoolConnectionFactoryStatistics {

    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private record JmsConnectionKey(String username, String password) {}

    private final Map<JmsConnectionKey, JmsKeyedConnectionPool> pools = new HashMap<>();

    /**
     * Indicates that periodic connection checks are not enabled and connections are only checked on
     * checkout and return.
     */
    public static final long PERIODIC_CONNECTION_CHECK_DISABLED = -1;

    /**
     * The default value controlling time between checks for idle connections in the pool.
     */
    public static final long DEFAULT_TIME_BETWEEN_EVICTION_RUNS = PERIODIC_CONNECTION_CHECK_DISABLED;

    /**
     * The minimum low bound for the time between eviction runs if not set to off (value is in milliseconds).
     */
    public static final long MIN_TIME_BETWEEN_EVICTION_RUNS = 1000;

    /**
     * The default maximum number of connections to maintain in the connection pool, this value
     * will be over-written by a call to set the max connections after the factory is created.
     */
    public static final int DEFAULT_MAX_CONNECTIONS = 1;

    private volatile int maxConnections = DEFAULT_MAX_CONNECTIONS;
    private volatile long connectionCheckInterval = DEFAULT_TIME_BETWEEN_EVICTION_RUNS;
    private ScheduledExecutorService scheduler;
    private volatile boolean stopped;

    /**
     * {@return the currently configured maximum connection to keep in the pool}
     */
    public int getMaxConnections() {
        return maxConnections;
    }

    /**
     * Sets the maximum number of connections that will be kept in the pool and shared
     * amongst callers. The minimum allowed value is one, and will always be set if a lower
     * value is passed. Any active connections in the pool beyond the new maximum are allowed
     * to continue operating and will only be closed once they are no longer referenced by
     * any user.
     *
     * @param maxConnections
     * 		The maximum number of connection to keep in the pool.
     */
    public synchronized void setMaxConnections(int maxConnections) {
        this.maxConnections = Math.max(DEFAULT_MAX_CONNECTIONS, maxConnections);
    }

    /**
     * {@return the currently configured connection check interval}
     */
    public long getConnectionCheckInterval() {
        return connectionCheckInterval;
    }

    /**
     * Sets the time between checks of connections in the pool for both connection state and idle
     * state validity. When the connection task runs it will check each connection to see if it
     * is currently idle (no-references) and if so if the idle time exceeds the configure maximum
     * idle period and if so close and remove that connection from the pool. If the connection
     * survives that check it will also attempt to probe the provider connection to ensure it is
     * still alive and usable and close and remove any connections that fail that check as well.
     *
     * @param connectionCheckInterval
     * 		The interval to use to perform connection validity checks.
     */
    public synchronized void setConnectionCheckInterval(long connectionCheckInterval) {
        if (this.connectionCheckInterval != connectionCheckInterval) {
            if (connectionCheckInterval >= 0) {
                connectionCheckInterval = Math.max(MIN_TIME_BETWEEN_EVICTION_RUNS, connectionCheckInterval);
            } else {
                connectionCheckInterval = PERIODIC_CONNECTION_CHECK_DISABLED;
            }

            this.connectionCheckInterval = connectionCheckInterval;

            if (scheduler != null) {
                scheduler.shutdownNow();
                scheduler = null;
            }

            scheduleIdleCheckTaskIfNeeded();
        }
    }

    @Override
    public synchronized int getConnectionCount() {
        final AtomicInteger count = new AtomicInteger();

        pools.values().forEach(pool -> count.addAndGet(pool.getNumConnections()));

        return count.get();
    }

    @Override
    public synchronized int getActiveConnectionCount() {
        final AtomicInteger count = new AtomicInteger();

        pools.values().forEach(pool -> count.addAndGet(pool.getNumActive()));

        return count.get();
    }

    @Override
    public synchronized int getIdleConnectionCount() {
        final AtomicInteger count = new AtomicInteger();

        pools.values().forEach(pool -> count.addAndGet(pool.getNumIdle()));

        return count.get();
    }

    @Override
    public synchronized int getConnectionCount(String username, String password) {
        final JmsConnectionKey key = new JmsConnectionKey(username, password);
        final JmsKeyedConnectionPool pool = pools.get(key);

        if (pool != null) {
            return pool.getNumConnections();
        } else {
            return 0;
        }
    }

    @Override
    public synchronized int getActiveConnectionCount(String username, String password) {
        final JmsConnectionKey key = new JmsConnectionKey(username, password);
        final JmsKeyedConnectionPool pool = pools.get(key);

        if (pool != null) {
            return pool.getNumActive();
        } else {
            return 0;
        }
    }

    @Override
    public synchronized int getIdleConnectionCount(String username, String password) {
        final JmsConnectionKey key = new JmsConnectionKey(username, password);
        final JmsKeyedConnectionPool pool = pools.get(key);

        if (pool != null) {
            return pool.getNumIdle();
        } else {
            return 0;
        }
    }

    @Override
    public synchronized void foreach(Consumer<JmsPoolConnectionStatistics> statsConsumer) {
        pools.values().forEach(pool -> {
            pool.connections.forEach(conn -> {
                try {
                    statsConsumer.accept(conn.getStatistics());
                } catch (Exception e) {
                    LOG.trace("Suppressed error from user connection statistics handler");
                }
            });
        });
    }

    @Override
    public synchronized void foreachActive(Consumer<JmsPoolConnectionStatistics> statsConsumer) {
        pools.values().forEach(pool -> {
            pool.connections.forEach(conn -> {
                if (!conn.isIdle()) {
                    try {
                        statsConsumer.accept(conn.getStatistics());
                    } catch (Exception e) {
                        LOG.trace("Suppressed error from user connection statistics handler");
                    }
                }
            });
        });
    }

    @Override
    public synchronized void foreachIdle(Consumer<JmsPoolConnectionStatistics> statsConsumer) {
        pools.values().forEach(pool -> {
            pool.connections.forEach(conn -> {
                if (conn.isIdle()) {
                    try {
                        statsConsumer.accept(conn.getStatistics());
                    } catch (Exception e) {
                        LOG.trace("Suppressed error from user connection statistics handler");
                    }
                }
            });
        });
    }

    /**
     * Returns the next available connection or creates a new one to add to the pool of connection.
     * <p>
     * Connections are created and loaned out individually up to the max connection limit and then they
     * are loaned on a rotating basis. If connection idle timeouts are configured then any connection that
     * is not currently loaned out to at least one caller is a candidate to be closed if it sits idle for
     * longer than the idle connection timeout.
     *
     * @param username
     * 		The user name that was used by the caller which makes up part of the connection pool key
     * @param password
     * 		The password that was used by the caller which makes up part of the connection pool key
     *
     * @return the next available connection from the pool
     *
     * @throws JMSException if a new connection cannot be created to match the demand
     */
    public synchronized CP nextAvailable(String username, String password) throws JMSException {
        if (isStopped()) {
            throw new IllegalStateException("Cannot create a connection because the connection pool is stopped.");
        }

        return pools.computeIfAbsent(
            new JmsConnectionKey(username, password), key -> new JmsKeyedConnectionPool(key)).nextAvailable();
    }

    /**
     * Closes and removes all connection from all sub-pools in this connection pool}
     */
    public synchronized void clear() {
        if (isStarted()) {
            pools.forEach((k, v) -> v.clear());
        }
    }

    /**
     * Idles the pool returning all connection into the internal free list for use when the
     * connection pool is used again by a newly created connection. These connections can be
     * idled out if the connection idle timeout is configured as the idle timeout checks will
     * continue to run
     */
    public synchronized void start() {
        if (isStopped()) {
            stopped = false;
            scheduleIdleCheckTaskIfNeeded();
        }
    }

    /**
     * Terminal operation that closes and destroys all pooled connections and cannot be undone.
     */
    public synchronized void stop() {
        if (isStarted()) {
            stopped = true;

            if (scheduler != null) {
                scheduler.shutdownNow();
                scheduler = null;
            }

            LOG.debug("Stopping the connection pool, number of connections in pool = {}", getConnectionCount());
            pools.forEach((k, v) -> v.clear());
        }
    }

    /**
     * {@return if the connection pool was stopped at the time this method was called}
     */
    public boolean isStopped() {
        return stopped;
    }

    /**
     * {@return if the connection pool was started at the time this method was called}
     */
    public boolean isStarted() {
        return !stopped;
    }

    /**
     * The pool concrete implementation will provide this method implementation and return a newly
     * allocated JMS session proxy that wraps a connection created using the provider connection
     * factory.
     *
     * @param username
     * 		The user name that was provided for the new connection
     * @param password
     * 		The password that was provided for the new connection
     * @param manager
     * 		The manager that will provide life-cycle APIs for the new connection proxy
     *
     * @return the newly create connection proxy instance that will be added to the pool.
     *
     * @throws JMSException if an error occurs while creating the new connection.
     */
    protected abstract CP createConnectionProxy(String username,
                                                String password,
                                                JmsConnectionProxyManager<CP> manager) throws JMSException;

    private void scheduleIdleCheckTaskIfNeeded() {
        if (connectionCheckInterval > 0 && isStarted()) {
            scheduler = Executors.newScheduledThreadPool(1);
            scheduler.scheduleWithFixedDelay(() -> {
                pools.values().forEach(pool -> pool.scheduledConnectionsCheck());
            }, connectionCheckInterval, connectionCheckInterval, TimeUnit.MILLISECONDS);
        }
    }

    private class JmsKeyedConnectionPool implements JmsConnectionProxyManager<CP> {

        private final Deque<CP> connections = new ArrayDeque<>();

        private final JmsConnectionKey connectionKey;

        JmsKeyedConnectionPool(JmsConnectionKey connectionKey) {
            this.connectionKey = connectionKey;
        }

        public void clear() {
            connections.forEach(conn -> {
                try {
                    conn.internalClose();
                } catch (Exception ex) {
                    LOG.trace("Suppressed an error while closing connection during clear", ex);
                }
            });
            connections.clear();
        }

        public int getNumConnections() {
            return connections.size();
        }

        public int getNumIdle() {
            final AtomicInteger count = new AtomicInteger();

            connections.forEach(conn -> {
                if (conn.isIdle()) {
                    count.incrementAndGet();
                }
            });

            return count.get();
        }

        public int getNumActive() {
            final AtomicInteger count = new AtomicInteger();

            connections.forEach(conn -> {
                if (!conn.isIdle()) {
                    count.incrementAndGet();
                }
            });

            return count.get();
        }

        public CP nextAvailable() throws JMSException {
            CP connection;

            do {
                if (connections.size() < maxConnections) {
                    connection = createConnectionProxy(connectionKey.username(),
                                                       connectionKey.password(),
                                                       this);

                    connections.offer(connection);
                } else {
                    connection = connections.poll();

                    // Connections can idle out while just sitting in the pool and need
                    // to be closed, likewise the provider connection could fail which is
                    // tested during this call.
                    if (connection.checkIsUsable()) {
                        // Rotates the connection to the end of the queue allowing other
                        // connections to share the load of client requests.
                        connections.offer(connection);
                    } else {
                        try {
                            connection.internalClose();
                        } catch (Exception e) {
                            LOG.trace("Suppressed an error while cleaning up a unusable connection during checkout", e);
                        } finally {
                            connection = null;
                        }
                    }
                }
            } while (connection == null);

            connection.acquire();

            return connection;
        }

        protected void scheduledConnectionsCheck() {
            // Start out empty and only grow when an actual failed or idle for to long
            // connection is found. We close out of band to release the lock and let new
            // connections enter the pool from create calls.
            final List<CP> connectionsToClose = new ArrayList<>(0);

            synchronized (JmsAbstractConnectionPool.this) {
                if (connections.isEmpty()) {
                    return;
                }

                connections.removeIf(connection -> {
                    final boolean remove = !connection.checkIsUsable();

                    if (remove) {
                        connectionsToClose.add(connection);
                    }

                    return remove;
                });
            }

            connectionsToClose.forEach(connection -> {
                try {
                    connection.internalClose();
                } catch (Exception e) {
                    LOG.trace("Suppressed an error while cleaning up a unusable connection during timed checks", e);
                }
            });
        }

        /*
         * Below calls are made from the pooled connection back into its manager and are required
         * to lock the parent connection pool instance to be thread safe against calls made to the
         * overall connection pool.
         */

        @Override
        public CP tryAcquireReference(CP connection) throws JMSException {
            boolean closeConnection = false;

            synchronized (JmsAbstractConnectionPool.this) {
                // If for some reason a caller trying to add registrations to a connection
                // encounters a connection that has already failed we just need to ensure we
                // have cleaned up and signal that the acquire attempt failed.
                if (connection.checkIsUsable()) {
                    connection.acquireReference();
                } else {
                    closeConnection = true;
                    connections.remove(connection);
                }
            }

            if (closeConnection) {
                try {
                    connection.internalClose();
                } catch (Exception e) {
                    LOG.trace("Suppressed an error while cleaning up a unusable connection during acquire", e);
                }

                throw new IllegalStateException("Connection is not in a usable state");
            }

            return connection;
        }

        @Override
        public CP tryReleaseReference(CP connection) {
            boolean closeConnection = false;

            synchronized (JmsAbstractConnectionPool.this) {
                connection.releaseReference();

                // Test on return of reference from any owner of the connection and if
                // the test fails remove it from the pool, we assume the other owners if
                // any will become aware as calls to connection or its resources fail or
                // will have seen any exception sent via the pooled connection exception
                // listener notifications.
                if (!connection.checkIsUsable()) {
                    closeConnection = true;
                    connections.remove(connection);
                }
            }

            if (closeConnection) {
                try {
                    connection.internalClose();
                } catch (Exception e) {
                    LOG.trace("Suppressed an error while cleaning up a unusable connection during release", e);
                }
            }

            return connection;
        }

        @Override
        public CP invalidateConnection(CP connection) {
            synchronized (JmsAbstractConnectionPool.this) {
                connections.remove(connection);
            }

            try {
                connection.internalClose();
            } catch (Exception e) {
                LOG.trace("Suppressed an error while cleaning up a closed connection at close", e);
            }

            return connection;
        }
    }
}
