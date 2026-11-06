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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.lang.invoke.MethodHandles;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.jms.Connection;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;
import jakarta.jms.Topic;

public class PooledSessionTest extends QpidJmsPoolTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Test
    public void testCloseSessionWhenConsumerWithMessageListenerStillOpen() throws Exception {
        final JmsPoolConnectionFactory cf = createPooledConnectionFactory();

        try {
            final Connection connection = cf.createConnection();
            final JmsPoolSession session = (JmsPoolSession) connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final Session providerSession1 = session.getProviderSession();

            session.setMessageListener((m) -> {
                LOG.info("Read message from session listener");
            });

            final Topic topic = session.createTopic(getTestName());
            final MessageConsumer consumer = session.createConsumer(topic);

            assertNotNull(consumer);

            assertDoesNotThrow(() -> session.close());

            final JmsPoolSession nextSession = (JmsPoolSession) connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final Session providerSession2 = nextSession.getProviderSession();

            assertSame(providerSession1, providerSession2);
        } finally {
            cf.stop();
        }
    }
}
