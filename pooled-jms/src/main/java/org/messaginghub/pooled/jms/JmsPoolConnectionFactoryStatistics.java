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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;

/**
 * Interface for the statistics API exposed for a JMS pooled connection factory
 * and the resources that it manages
 */
public interface JmsPoolConnectionFactoryStatistics {

    /**
     * Returns the total number of connection that are in the JMS connection pool
     * managed by the parent JMS pool {@link ConnectionFactory}.
     *
     * The pool is actually a collection of sub-pools divided by the credentials
     * that define each sub-pool the number returned reflects all connections in all
     * sub-pools including idle connections in the pools.
     *
     * @return the current number of connections in the connection pool
     */
    int getConnectionCount();

    /**
     * Returns the total number of active connection that are in the JMS connection
     * pool managed by the parent JMS pooled {@link ConnectionFactory}.
     *
     * The pool is actually a collection of sub-pools divided by the credentials
     * that define each sub-pool the number returned reflects all active connections
     * in all sub-pools where active means the connection is loaned to at least one user.
     *
     * @return the current number of active connections in the connection pool
     */
    int getActiveConnectionCount();

    /**
     * Returns the total number of idle connection that are in the JMS connection
     * pool managed by the parent JMS pooled {@link ConnectionFactory}.
     *
     * The pool is actually a collection of sub-pools divided by the credentials
     * that define each sub-pool the number returned reflects all idle connections
     * in all sub-pools.
     *
     * @return the current number of idle connections in the connection pool
     */
    int getIdleConnectionCount();

    /**
     * Returns the total number of connection that are in the JMS connection pool
     * managed by the parent JMS pool {@link ConnectionFactory} for the given user
     * name and password combination.
     *
     * The pool is actually a collection of sub-pools divided by the credentials
     * that define each sub-pool the number returned reflects the connections in the
     * sub-pool associated with the given user name and password including idle
     * connections in that pool.
     *
     * @param username
     * 		The user name to use to filter for a given connection pool
     * @param password
     * 		The password to use to filter for a given connection pool
     *
     * @return the current number of connections in the scoped connection pool
     */
    int getConnectionCount(String username, String password);

    /**
     * Returns the total number of active connection that are in the JMS connection pool
     * managed by the parent JMS pool {@link ConnectionFactory} for the given user
     * name and password combination.
     *
     * The pool is actually a collection of sub-pools divided by the credentials
     * that define each sub-pool the number returned reflects all active connections
     * in all sub-pools where active means the connection is loaned to at least one user.
     *
     * @param username
     * 		The user name to use to filter for a given connection pool
     * @param password
     * 		The password to use to filter for a given connection pool
     *
     * @return the current number of active connections in the scoped connection pool
     */
    int getActiveConnectionCount(String username, String password);

    /**
     * Returns the total number of idle connection that are in the JMS connection pool
     * managed by the parent JMS pool {@link ConnectionFactory} for the given user
     * name and password combination.
     *
     * The pool is actually a collection of sub-pools divided by the credentials
     * that define each sub-pool the number returned reflects the idle connections in
     * the sub-pool associated with the given user name and password.
     *
     * @param username
     * 		The user name to use to filter for a given connection pool
     * @param password
     * 		The password to use to filter for a given connection pool
     *
     * @return the current number of idle connections in the scoped connection pool
     */
    int getIdleConnectionCount(String username, String password);

    /**
     * Performs the given processing for the connection statistics associated with each
     * distinct pooled JMS connection currently held in the JMS {@link Connection} pool.
     *
     * @param statsConsumer
     * 		Consumer that consumes statistics from each connection in the connection pool
     */
    void foreach(Consumer<JmsPoolConnectionStatistics> statsConsumer);

    /**
     * Performs the given processing for the connection statistics associated with each
     * active pooled JMS connection currently held in the JMS {@link Connection} pool.
     *
     * @param statsConsumer
     * 		Consumer that consumes statistics from each connection in the connection pool
     */
    void foreachActive(Consumer<JmsPoolConnectionStatistics> statsConsumer);

    /**
     * Performs the given processing for the connection statistics associated with each
     * idle pooled JMS connection currently held in the JMS {@link Connection} pool.
     *
     * @param statsConsumer
     * 		Consumer that consumes statistics from each connection in the connection pool
     */
    void foreachIdle(Consumer<JmsPoolConnectionStatistics> statsConsumer);

    /**
     * Returns the total number of sessions that are currently in the session pool
     * of each connection managed by the parent JMS connections pool.
     *
     * The session pool of a connection is actually a collection of sub-pools divided
     * by the transaction mode and acknowledgement mode of the sessions created from
     * a pooled connection. The returned value reflects the count of all sessions in
     * all sub-pools of all connections in the pool.
     *
     * @return the current number of session in the JMS pool in total
     */
    default int getSessionCount() {
        final AtomicInteger sessions = new AtomicInteger();

        foreach(statistics -> {
            sessions.addAndGet(statistics.getSessionCount());
        });

        return sessions.get();
    }

    /**
     * Returns the total number of active sessions that are currently in the session pool
     * of each connection managed by the parent JMS connections pool.
     *
     * The session pool of a connection is actually a collection of sub-pools divided
     * by the transaction mode and acknowledgement mode of the sessions created from a
     * pooled connection. The returned value reflects the count of all actively loaned out
     * sessions in all sub-pools of all connections in the pool.
     *
     * @return the current number of active session in the JMS pool in total
     */
    default int getActiveSessionCount() {
        final AtomicInteger sessions = new AtomicInteger();

        // Only actively loaned connection would have actively loaned sessions so we
        // can reduce the work by only looking at those active connections.

        foreachActive(statistics -> {
            sessions.addAndGet(statistics.getActiveSessionCount());
        });

        return sessions.get();
    }

    /**
     * Returns the total number of idle sessions that are currently in the session pool
     * of each connection managed by the parent JMS connections pool.
     *
     * The session pool of a connection is actually a collection of sub-pools divided
     * by the transaction mode and acknowledgement mode of the sessions created from a
     * pooled connection. The returned value reflects the count of all not currently loaned
     * out sessions in all sub-pools of all connections in the pool.
     *
     * @return the current number of idle session in the JMS pool in total
     */
    default int getIdleSessionCount() {
        final AtomicInteger sessions = new AtomicInteger();

        foreach(statistics -> {
            sessions.addAndGet(statistics.getIdleSessionCount());
        });

        return sessions.get();
    }
}
