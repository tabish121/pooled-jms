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

import java.util.function.Consumer;

import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.Session;

/**
 * Non-XA JMS Connection proxy type.
 */
public class JmsPoolConnectionProxy extends JmsPoolAbstractConnectionProxy<JmsPoolConnectionProxy, JmsPoolSessionProxy> {

    JmsPoolConnectionProxy(JmsPoolConnectionConfiguration configuration, Connection connection) {
        super(configuration, connection);
    }

    @Override
    protected JmsPoolConnectionProxy self() {
        return this;
    }

    @Override
    protected JmsPoolSessionsPool createSessionPool(JmsPoolConnectionConfiguration configuration) {
        return new JmsPoolSessionsPool(configuration);
    }

    private class JmsPoolSessionsPool extends JmsPoolAbstractSessionPool<JmsPoolSessionProxy> {

        JmsPoolSessionsPool(JmsPoolConnectionConfiguration configuration) {
            super(configuration);
        }

        @Override
        protected JmsPoolSessionProxy createSessionProxy(boolean transacted, int sessionMode,
                                                         Consumer<JmsPoolSessionProxy> onSessionClosed,
                                                         Consumer<JmsPoolSessionProxy> onSessionDestroyed) throws JMSException {
            final Session session = getConnection().createSession(transacted, sessionMode);

            return new JmsPoolSessionProxy(getConfiguration(), getVersionSupport(), session, onSessionClosed, onSessionDestroyed);
        }
    }
}
