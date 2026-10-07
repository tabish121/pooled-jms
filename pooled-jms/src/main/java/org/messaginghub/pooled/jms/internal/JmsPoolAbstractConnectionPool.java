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
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.jms.IllegalStateException;
import jakarta.jms.JMSException;

/**
 * Base for the connection pool used to manage loaned and free connections
 */
public abstract class JmsPoolAbstractConnectionPool<CP extends JmsPoolAbstractConnectionProxy<?, ?>> {

    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final Map<JmsPoolConnectionKey, JmsPoolKeyedConnectionPool> pools = new HashMap<>();

    /**
     * The default value controlling time between checks for idle connections in the pool.
     */
    public static final long DEFAULT_TIME_BETWEEN_EVICTION_RUNS = -1;

    /**
     * The default maximum number of connections to maintain in the connection pool, this value
     * will be over-written by a call to set the max connections after the factory is created.
     */
    public static final int DEFAULT_MAX_CONNECTIONS = 1;

    private int maxConnections = DEFAULT_MAX_CONNECTIONS;
    private long connectionCheckInterval = DEFAULT_TIME_BETWEEN_EVICTION_RUNS;
    private ScheduledExecutorService scheduler;
    private boolean stopped;

    JmsPoolAbstractConnectionPool() {}

    public int getMaxConnections() {
        return maxConnections;
    }

    public synchronized void setMaxConnections(int maxConnections) {
        this.maxConnections = maxConnections;
    }

    public long getConnectionCheckInterval() {
        return connectionCheckInterval;
    }

    public synchronized void setConnectionCheckInterval(long connectionCheckInterval) {
        if (this.connectionCheckInterval == connectionCheckInterval) {
            return;
        }

        this.connectionCheckInterval = connectionCheckInterval;

        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }

        scheduleIdleCheckTaskIfNeeded();
    }

    /**
     * {@return the total number of connections that are currently idle and active in this connection pool}
     */
    public synchronized int getConnectionCount() {
        final AtomicInteger count = new AtomicInteger();

        pools.values().forEach(pool -> count.addAndGet(pool.getNumConnections()));

        return count.get();
    }

    /**
     * {@return the number of connections that are currently idle in this connection pool}
     */
    public synchronized int getIdleConnectionCount() {
        final AtomicInteger count = new AtomicInteger();

        pools.values().forEach(pool -> count.addAndGet(pool.getNumIdle()));

        return count.get();
    }

    /**
     * {@return the number of connections that are currently loaned out from this connection pool}
     */
    public synchronized int getActiveCountCount() {
        final AtomicInteger count = new AtomicInteger();

        pools.values().forEach(pool -> count.addAndGet(pool.getNumActive()));

        return count.get();
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
            new JmsPoolConnectionKey(username, password), key -> new JmsPoolKeyedConnectionPool(key)).nextAvailable();
    }

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

    protected abstract CP createConnectionProxy(String username, String password,
                                                Consumer<CP> onConnectionClosed,
                                                Consumer<CP> onConnectionDestroyed) throws JMSException;

    private void scheduleIdleCheckTaskIfNeeded() {
        if (connectionCheckInterval > 0 && isStarted()) {
            scheduler = Executors.newScheduledThreadPool(1);
            scheduler.scheduleWithFixedDelay(() -> {
                pools.values().forEach(pool -> pool.scheduledConnectionsCheck());
            }, connectionCheckInterval, connectionCheckInterval, TimeUnit.MILLISECONDS);
        }
    }

    private class JmsPoolKeyedConnectionPool {

        private final Deque<CP> connections = new ArrayDeque<>();

        private final JmsPoolConnectionKey connectionKey;

        private boolean selfTriggeredCallbacks;

        JmsPoolKeyedConnectionPool(JmsPoolConnectionKey connectionKey) {
            this.connectionKey = connectionKey;
        }

        public void clear() {
            selfTriggeredCallbacks = true;

            try {
                connections.forEach(conn -> {
                    try {
                        conn.destroy();
                    } catch (Exception ex) {
                        // Ignore provider errors on close.
                    }
                });
                connections.clear();
            } finally {
                selfTriggeredCallbacks = false;
            }
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
                    connection = createConnectionProxy(connectionKey.getUserName(),
                                                       connectionKey.getPassword(),
                                                       this::onConnectionClosed,
                                                       this::onConnectionDestroyed);

                    connections.offer(connection);
                } else {
                    connection = connections.poll();

                    // Connections can idle out while just sitting in the pool and need
                    // to be closed, likewise the provider connection could fail which is
                    // tested during this call.
                    if (!connection.checkIsUsable()) {
                        selfTriggeredCallbacks = true;

                        try {
                            connection.destroy();
                        } catch (Exception e) {
                            // Error on destroy are ignored
                        } finally {
                            selfTriggeredCallbacks = false;
                            connection = null;
                        }
                    } else {
                        // Rotates the connection to the end of the queue allowing other
                        // connections to share the load of client requests.
                        connections.offer(connection);
                    }
                }
            } while (connection == null);

            connection.acquire();

            return connection;
        }

        protected void scheduledConnectionsCheck() {
            synchronized (JmsPoolAbstractConnectionPool.this) {
                connections.removeIf(connection -> {
                    final boolean remove = connection.checkIsUsable();

                    if (remove) {
                        selfTriggeredCallbacks = true;

                        try {
                            connection.destroy();
                        } catch (Exception e) {
                            // Error on destroy are ignored
                        } finally {
                            selfTriggeredCallbacks = false;
                        }
                    }

                    return remove;
                });
            }
        }

        protected void onConnectionClosed(CP connection) {
            synchronized (JmsPoolAbstractConnectionPool.this) {
                if (!selfTriggeredCallbacks) {
                    if (!connection.checkIsUsable()) {
                        try {
                            connection.destroy();
                        } catch (Exception e) {
                            // Error on destroy are ignored
                        }

                        connections.remove(connection);
                    }
                }
            }
        }

        protected void onConnectionDestroyed(CP connection) {
            synchronized (JmsPoolAbstractConnectionPool.this) {
                if (!selfTriggeredCallbacks) {
                    connections.remove(connection);
                }
            }
        }
    }
}
