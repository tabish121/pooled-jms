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

        if (connectionCheckInterval > 0 && isStarted()) {
            scheduler = Executors.newScheduledThreadPool(1);
            scheduler.scheduleWithFixedDelay(() -> {
                pools.values().forEach(pool -> pool.scheduledConnectionsCheck());
            }, connectionCheckInterval, connectionCheckInterval, TimeUnit.MILLISECONDS);
        } else if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
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

            if (connectionCheckInterval > 0) {
                scheduler = Executors.newScheduledThreadPool(1);
                scheduler.scheduleWithFixedDelay(() -> {
                    pools.values().forEach(pool -> pool.scheduledConnectionsCheck());
                }, connectionCheckInterval, connectionCheckInterval, TimeUnit.MILLISECONDS);
            }
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

                        connections.remove();
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

//  GenericKeyedObjectPool<JmsPoolConnectionKey, CP> getConnectionsPool() {
//  if (!isStopped() && connectionsPool == null) {
//      final GenericKeyedObjectPoolConfig<CP> poolConfig = new GenericKeyedObjectPoolConfig<>();
//      poolConfig.setJmxEnabled(false);
//
//      connectionsPool = new GenericKeyedObjectPool<JmsPoolConnectionKey, CP>(
//          new KeyedPooledObjectFactory<JmsPoolConnectionKey, CP>() {
//              @Override
//              public PooledObject<CP> makeObject(JmsPoolConnectionKey connectionKey) throws Exception {
//                  final Connection delegate = createProviderConnection(connectionKey.getUserName(), connectionKey.getPassword());
//                  final CP connection = createConnectionProxy(configuration.snapshot(), delegate);
//
//                  LOG.trace("Created new connection: {}", connection);
//                  JmsPoolAbstractConnectionProxyFactory.this.mostRecentlyCreated.set(connection);
//
//                  return new DefaultPooledObject<CP>(connection);
//              }
//
//              @Override
//              public void destroyObject(JmsPoolConnectionKey connectionKey, PooledObject<CP> pooledObject) throws Exception {
//                  final CP connection = pooledObject.getObject();
//
//                  try {
//                      LOG.trace("Destroying connection: {}", connection);
//                      connection.destroy();
//                  } catch (Exception e) {
//                      LOG.warn("Close connection failed for connection: " + connection + ". This exception will be ignored.",e);
//                  }
//              }
//
//              @Override
//              public boolean validateObject(JmsPoolConnectionKey connectionKey, PooledObject<CP> pooledObject) {
//                  final CP connection = pooledObject.getObject();
//
//                  return connection == null ? false : connection.checkIsUsable();
//              }
//
//              @Override
//              public void activateObject(JmsPoolConnectionKey connectionKey, PooledObject<CP> pooledObject) throws Exception {
//              }
//
//              @Override
//              public void passivateObject(JmsPoolConnectionKey connectionKey, PooledObject<CP> pooledObject) throws Exception {
//              }
//
//          }, poolConfig);
//
//      // Set max idle (not max active) since our connections always idle in the pool.
//      connectionsPool.setMaxIdlePerKey(1);
//      connectionsPool.setMinIdlePerKey(1); // Always want one connection pooled.
//      connectionsPool.setLifo(false);
//      connectionsPool.setBlockWhenExhausted(false);
//      connectionsPool.setDurationBetweenEvictionRuns(Duration.ofMillis(-1));
//      connectionsPool.setMinEvictableIdleDuration(Duration.ofMillis(Long.MAX_VALUE));
//      connectionsPool.setTestOnBorrow(true);
//      connectionsPool.setTestWhileIdle(true);
//      connectionsPool.setTestOnReturn(true);
//
//      // Don't use the default eviction policy as it ignores our own idle timeout option.
//      final EvictionPolicy<CP> policy = new EvictionPolicy<>() {
//
//          @Override
//          public boolean evict(EvictionConfig config, PooledObject<CP> underTest, int idleCount) {
//              return false; // We use the validation of the instance to check for idle.
//          }
//      };
//
//      connectionsPool.setEvictionPolicy(policy);
//  }
//
//  return connectionsPool;
//}
//
//private synchronized CP createJmsPoolConnection(String userName, String password) throws JMSException {
//  if (isStopped()) {
//      LOG.debug("The JMS pooling connection factoring is stopped, skipping create new connection.");
//      throw new IllegalStateException("Cannot create a new JMS connection from a stopped pooled connection factory");
//  }
//
//  if (getConnectionFactory() == null) {
//      throw new IllegalStateException("No JMS client ConnectionFactory instance has been configured");
//  }
//
//  final JmsPoolConnectionKey key = new JmsPoolConnectionKey(userName, password);
//  CP connection = null;
//
//  // Place a new idle connection into the pool as we are under the limit, once we reach
//  // the limit the pool will be in FIFO mode and the least most used entry in the pool
//  // will be returned but until then it will be in LIFO mode and the most recently used
//  // entry will be added
//  if (getConnectionsPool().getNumIdle(key) < getMaxConnections()) {
//      try {
//          connectionsPool.addObject(key);
//          connection = mostRecentlyCreated.getAndSet(null);
//      } catch (Exception e) {
//          throw JMSExceptionSupport.create("Error while attempting to add new Connection to the pool", e);
//      }
//  }
//
//  if (connection == null) {
//      try {
//          int exhaustedPoolRecoveryAttempts = 0;
//          long exhaustedPoolRecoveryBackoff = EXHAUSTION_RECOVER_INITIAL_BACKOFF;
//
//          // We can race against other threads returning the connection when there is an
//          // expiration or idle timeout.  We keep pulling out ConnectionPool instances until
//          // we win and get a non-closed instance and then increment the reference count
//          // under lock to prevent another thread from triggering an expiration check and
//          // pulling the rug out from under us.
//          while (connection == null) {
//              try {
//                  connection = connectionsPool.borrowObject(key);
//              } catch (NoSuchElementException nse) {
//                  if (exhaustedPoolRecoveryAttempts++ < EXHUASTION_RECOVER_RETRY_LIMIT) {
//                      LOG.trace("Recover attempt {} from exhausted pool by refilling pool key and creating new Connection", exhaustedPoolRecoveryAttempts);
//                      if (exhaustedPoolRecoveryAttempts > 1) {
//                          LockSupport.parkNanos(exhaustedPoolRecoveryBackoff);
//                          exhaustedPoolRecoveryBackoff = Math.min(EXHAUSTION_RECOVER_BACKOFF_LIMIT,
//                                                                  exhaustedPoolRecoveryBackoff + exhaustedPoolRecoveryBackoff);
//                      } else {
//                          Thread.yield();
//                      }
//
//                      connectionsPool.addObject(key);
//                      continue;
//                  } else {
//                      throw JMSExceptionSupport.createResourceAllocationException(nse);
//                  }
//              }
//              synchronized (connection) {
//                  if (connection.isClosed()) {
//                      // Return the bad one to the pool and let if get destroyed as normal.
//                      connectionsPool.returnObject(key, connection);
//                      connection = null;
//                  }
//              }
//          }
//      } catch (Exception e) {
//          throw JMSExceptionSupport.create("Error while attempting to retrieve a connection from the pool", e);
//      }
//
//      try {
//          connectionsPool.returnObject(key, connection);
//      } catch (Exception e) {
//          throw JMSExceptionSupport.create("Error when returning connection to the pool", e);
//      }
//
//      connection.acquire();
//  }
//
//  return connection;
//}
}
