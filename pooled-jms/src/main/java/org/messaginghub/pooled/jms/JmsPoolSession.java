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

import org.messaginghub.pooled.jms.internal.JmsPoolSessionProxy;

import jakarta.jms.JMSException;

/**
 * Session that has been taken from a pool of sessions maintained by a pooled JMS Connection.
 * <p>
 * The application code has full ownership of the pooled session instance until it closes its
 * wrapper object at which time the session is returned to the connection's pool for use by a
 * new call to create a session.
 */
public class JmsPoolSession extends JmsPoolAbstractSession {

    JmsPoolSession(JmsPoolSessionProxy session, boolean transactional) {
        super(session, transactional);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " { " + session + " }";
    }

    @Override
    protected JmsPoolSessionProxy safeGetSessionProxy() throws JMSException {
        checkClosed();
        return (JmsPoolSessionProxy) this.session;
    }
}
