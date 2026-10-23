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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import jakarta.jms.Connection;
import jakarta.jms.Session;

/**
 * Performs basic tests on the JmsPoolConnectionStatistics implementation.
 */
@Timeout(60)
public class JmsPoolConnectionStatisticsTest extends JmsPoolTestSupport {

    @Test
    public void testUnusedConnectionReportsNoSessions() throws  Exception {
        final Connection connection = cf.createConnection();

        assertNotNull(connection);

        final JmsPoolConnectionStatistics statistics = ((JmsPoolConnection) connection).getStatistics();

        assertNotNull(statistics);

        assertEquals(0, statistics.getSessionCount());
        assertEquals(0, statistics.getActiveSessionCount());
        assertEquals(0, statistics.getIdleSessionCount());
    }

    @Test
    public void testConnectionReportsSessionsInCorrectState() throws  Exception {
        final Connection connection = cf.createConnection();

        assertNotNull(connection);

        final JmsPoolConnectionStatistics statistics = ((JmsPoolConnection) connection).getStatistics();

        assertNotNull(statistics);

        assertEquals(0, statistics.getSessionCount());
        assertEquals(0, statistics.getActiveSessionCount());
        assertEquals(0, statistics.getIdleSessionCount());

        final Session session = connection.createSession();

        assertNotNull(session);

        assertEquals(1, statistics.getSessionCount());
        assertEquals(1, statistics.getActiveSessionCount());
        assertEquals(0, statistics.getIdleSessionCount());

        session.close();

        assertEquals(1, statistics.getSessionCount());
        assertEquals(0, statistics.getActiveSessionCount());
        assertEquals(1, statistics.getIdleSessionCount());

        connection.close();

        assertEquals(1, statistics.getSessionCount());
        assertEquals(0, statistics.getActiveSessionCount());
        assertEquals(1, statistics.getIdleSessionCount());
    }

    @Test
    public void testConnectionReportsSessionIdleAfterPooledConnectionClosed() throws  Exception {
        final Connection connection = cf.createConnection();

        assertNotNull(connection);

        final JmsPoolConnectionStatistics statistics = ((JmsPoolConnection) connection).getStatistics();

        assertNotNull(statistics);

        assertEquals(0, statistics.getSessionCount());
        assertEquals(0, statistics.getActiveSessionCount());
        assertEquals(0, statistics.getIdleSessionCount());

        final Session session = connection.createSession();

        assertNotNull(session);

        assertEquals(1, statistics.getSessionCount());
        assertEquals(1, statistics.getActiveSessionCount());
        assertEquals(0, statistics.getIdleSessionCount());

        connection.close();

        assertEquals(1, statistics.getSessionCount());
        assertEquals(0, statistics.getActiveSessionCount());
        assertEquals(1, statistics.getIdleSessionCount());

        cf.stop();

        assertEquals(0, statistics.getSessionCount());
        assertEquals(0, statistics.getActiveSessionCount());
        assertEquals(0, statistics.getIdleSessionCount());
    }
}
