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

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import jakarta.jms.Connection;
import jakarta.jms.Session;

/**
 * Performs basic tests on the JmsPoolConnectionFactoryStatistics implementation.
 */
@Timeout(60)
public class JmsPoolConnectionFactoryStatisticsTest extends JmsPoolTestSupport {

    @Test
    public void testUnusedPoolReportsNoConnections() throws  Exception {
        assertEquals(0, cf.getNumConnections());
        assertEquals(0, cf.getNumIdleConnections());
        assertEquals(0, cf.getNumActiveConnections());

        assertEquals(0, cf.getStatistics().getConnectionCount());
        assertEquals(0, cf.getStatistics().getIdleConnectionCount());
        assertEquals(0, cf.getStatistics().getActiveConnectionCount());

        assertEquals(0, cf.getStatistics().getSessionCount());
        assertEquals(0, cf.getStatistics().getIdleSessionCount());
        assertEquals(0, cf.getStatistics().getActiveSessionCount());

        assertEquals(0, cf.getStatistics().getConnectionCount("test", "test"));
        assertEquals(0, cf.getStatistics().getIdleConnectionCount("test", "test"));
        assertEquals(0, cf.getStatistics().getActiveConnectionCount("test", "test"));
    }

    @Test
    public void testUsedPoolReportsSingleConnectionWithNoCredentials() throws  Exception {
        final Connection connection = cf.createConnection();

        assertNotNull(connection);

        assertEquals(1, cf.getNumConnections());
        assertEquals(0, cf.getNumIdleConnections());
        assertEquals(1, cf.getNumActiveConnections());

        assertEquals(1, cf.getStatistics().getConnectionCount());
        assertEquals(0, cf.getStatistics().getIdleConnectionCount());
        assertEquals(1, cf.getStatistics().getActiveConnectionCount());

        assertEquals(1, cf.getStatistics().getConnectionCount(null, null));
        assertEquals(0, cf.getStatistics().getIdleConnectionCount(null, null));
        assertEquals(1, cf.getStatistics().getActiveConnectionCount(null, null));

        connection.close();

        assertEquals(1, cf.getStatistics().getConnectionCount());
        assertEquals(1, cf.getStatistics().getIdleConnectionCount());
        assertEquals(0, cf.getStatistics().getActiveConnectionCount());

        assertEquals(1, cf.getStatistics().getConnectionCount(null, null));
        assertEquals(1, cf.getStatistics().getIdleConnectionCount(null, null));
        assertEquals(0, cf.getStatistics().getActiveConnectionCount(null, null));
    }

    @Test
    public void testUsedPoolReportsSingleConnectionWithCredentials() throws  Exception {
        final Connection connection = cf.createConnection("test", "test");

        assertNotNull(connection);

        assertEquals(1, cf.getNumConnections());
        assertEquals(0, cf.getNumIdleConnections());
        assertEquals(1, cf.getNumActiveConnections());

        assertEquals(1, cf.getStatistics().getConnectionCount());
        assertEquals(0, cf.getStatistics().getIdleConnectionCount());
        assertEquals(1, cf.getStatistics().getActiveConnectionCount());

        assertEquals(1, cf.getStatistics().getConnectionCount("test", "test"));
        assertEquals(0, cf.getStatistics().getIdleConnectionCount("test", "test"));
        assertEquals(1, cf.getStatistics().getActiveConnectionCount("test", "test"));

        connection.close();

        assertEquals(1, cf.getStatistics().getConnectionCount());
        assertEquals(1, cf.getStatistics().getIdleConnectionCount());
        assertEquals(0, cf.getStatistics().getActiveConnectionCount());

        assertEquals(1, cf.getStatistics().getConnectionCount("test", "test"));
        assertEquals(1, cf.getStatistics().getIdleConnectionCount("test", "test"));
        assertEquals(0, cf.getStatistics().getActiveConnectionCount("test", "test"));
    }

    @Test
    public void testForEachOverTwoActiveConnections() throws  Exception {
        final Connection connection1 = cf.createConnection("test", "test1");
        final Connection connection2 = cf.createConnection("test", "test2");
        final Connection connection3 = cf.createConnection("test", "test3");

        assertNotNull(connection1);
        assertNotNull(connection2);
        assertNotNull(connection3);

        final Session s1c1 = connection1.createSession();
        final Session s2c1 = connection1.createSession();
        final Session s1c2 = connection2.createSession();
        final Session s1c3 = connection3.createSession();

        assertNotNull(s1c1);
        assertNotNull(s2c1);
        assertNotNull(s1c2);
        assertNotNull(s1c3);

        final AtomicInteger sessions = new AtomicInteger();

        cf.getStatistics().foreach(connectionStats -> {
            sessions.addAndGet(connectionStats.getActiveSessionCount());
        });

        assertEquals(4, sessions.get());

        sessions.set(0);
        s2c1.close();

        cf.getStatistics().foreach(connectionStats -> {
            sessions.addAndGet(connectionStats.getActiveSessionCount());
        });

        assertEquals(3, sessions.get());

        sessions.set(0);
        connection2.close();
        connection3.close();

        cf.getStatistics().foreach(connectionStats -> {
            sessions.addAndGet(connectionStats.getActiveSessionCount());
        });

        assertEquals(1, sessions.get());
    }

    @Test
    public void testForEachStatisticsUpdatesAsConnectionsChange() throws  Exception {
        final Connection connection1 = cf.createConnection("test", "test1");
        final Connection connection2 = cf.createConnection("test", "test2");
        final Connection connection3 = cf.createConnection("test", "test3");

        assertNotNull(connection1);
        assertNotNull(connection2);
        assertNotNull(connection3);

        final AtomicInteger connections = new AtomicInteger();
        final AtomicInteger active = new AtomicInteger();
        final AtomicInteger idle = new AtomicInteger();

        cf.getStatistics().foreach(connectionStats -> {
            connections.incrementAndGet();
        });

        assertEquals(3, connections.get());

        connections.set(0);
        active.set(0);
        idle.set(0);

        cf.getStatistics().foreachActive(connectionStats -> {
            connections.incrementAndGet();
        });

        assertEquals(3, connections.get());

        connections.set(0);
        active.set(0);
        idle.set(0);

        cf.getStatistics().foreachIdle(connectionStats -> {
            connections.incrementAndGet();
        });

        assertEquals(0, connections.get());

        connection1.close();

        connections.set(0);
        active.set(0);
        idle.set(0);

        cf.getStatistics().foreach(connectionStats -> {
            connections.incrementAndGet();
        });
        cf.getStatistics().foreachIdle(connectionStats -> {
            idle.incrementAndGet();
        });
        cf.getStatistics().foreachActive(connectionStats -> {
            active.incrementAndGet();
        });

        assertEquals(3, connections.get());
        assertEquals(2, active.get());
        assertEquals(1, idle.get());

        connection2.close();

        connections.set(0);
        active.set(0);
        idle.set(0);

        cf.getStatistics().foreach(connectionStats -> {
            connections.incrementAndGet();
        });
        cf.getStatistics().foreachIdle(connectionStats -> {
            idle.incrementAndGet();
        });
        cf.getStatistics().foreachActive(connectionStats -> {
            active.incrementAndGet();
        });

        assertEquals(3, connections.get());
        assertEquals(1, active.get());
        assertEquals(2, idle.get());

        connection3.close();

        connections.set(0);
        active.set(0);
        idle.set(0);

        cf.getStatistics().foreach(connectionStats -> {
            connections.incrementAndGet();
        });
        cf.getStatistics().foreachIdle(connectionStats -> {
            idle.incrementAndGet();
        });
        cf.getStatistics().foreachActive(connectionStats -> {
            active.incrementAndGet();
        });

        assertEquals(3, connections.get());
        assertEquals(0, active.get());
        assertEquals(3, idle.get());

        final Connection connection4 = cf.createConnection("test", "test1");

        assertNotNull(connection4);

        connections.set(0);
        active.set(0);
        idle.set(0);

        cf.getStatistics().foreach(connectionStats -> {
            connections.incrementAndGet();
        });
        cf.getStatistics().foreachIdle(connectionStats -> {
            idle.incrementAndGet();
        });
        cf.getStatistics().foreachActive(connectionStats -> {
            active.incrementAndGet();
        });

        assertEquals(3, connections.get());
        assertEquals(1, active.get());
        assertEquals(2, idle.get());
    }

    @Test
    public void testGetSessionCountsFromCFStatistics() throws  Exception {
        final Connection connection1 = cf.createConnection("test", "test1");
        final Connection connection2 = cf.createConnection("test", "test2");
        final Connection connection3 = cf.createConnection("test", "test3");

        assertNotNull(connection1);
        assertNotNull(connection2);
        assertNotNull(connection3);

        assertEquals(0, cf.getStatistics().getSessionCount());
        assertEquals(0, cf.getStatistics().getIdleSessionCount());
        assertEquals(0, cf.getStatistics().getActiveSessionCount());

        final Session s1c1 = connection1.createSession();
        final Session s1c2 = connection2.createSession();
        final Session s1c3 = connection3.createSession();

        assertNotNull(s1c1);
        assertNotNull(s1c2);
        assertNotNull(s1c3);

        assertEquals(3, cf.getStatistics().getSessionCount());
        assertEquals(0, cf.getStatistics().getIdleSessionCount());
        assertEquals(3, cf.getStatistics().getActiveSessionCount());

        s1c1.close();

        assertEquals(3, cf.getStatistics().getSessionCount());
        assertEquals(1, cf.getStatistics().getIdleSessionCount());
        assertEquals(2, cf.getStatistics().getActiveSessionCount());

        s1c2.close();

        assertEquals(3, cf.getStatistics().getSessionCount());
        assertEquals(2, cf.getStatistics().getIdleSessionCount());
        assertEquals(1, cf.getStatistics().getActiveSessionCount());

        s1c3.close();

        assertEquals(3, cf.getStatistics().getSessionCount());
        assertEquals(3, cf.getStatistics().getIdleSessionCount());
        assertEquals(0, cf.getStatistics().getActiveSessionCount());

        connection1.close();

        assertEquals(3, cf.getStatistics().getSessionCount());
        assertEquals(3, cf.getStatistics().getIdleSessionCount());
        assertEquals(0, cf.getStatistics().getActiveSessionCount());

        final Session s2c2 = connection2.createSession();

        assertNotNull(s2c2);

        assertEquals(3, cf.getStatistics().getSessionCount());
        assertEquals(2, cf.getStatistics().getIdleSessionCount());
        assertEquals(1, cf.getStatistics().getActiveSessionCount());

        cf.stop();

        assertEquals(0, cf.getStatistics().getSessionCount());
        assertEquals(0, cf.getStatistics().getIdleSessionCount());
        assertEquals(0, cf.getStatistics().getActiveSessionCount());
    }
}
