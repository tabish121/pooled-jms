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

import javax.transaction.xa.XAResource;

import org.messaginghub.pooled.jms.util.JMSVersionSupport;

import jakarta.jms.JMSException;
import jakarta.jms.QueueSession;
import jakarta.jms.Session;
import jakarta.jms.TopicSession;
import jakarta.jms.XAQueueSession;
import jakarta.jms.XASession;
import jakarta.jms.XATopicSession;

/**
 * The XA aware session proxy returned by pooled XA connection instances.
 */
public class JmsXASessionProxy extends JmsAbstractSessionProxy<JmsXASessionProxy> implements XASession, XATopicSession, XAQueueSession {

    private final XASession session;

    /**
     * Creates the XA session proxy wrapper around the given {@link XASession} instance.
     *
     * @param configuration
     * 		The immutable configuration applied to this session
     * @param versionSupport
     * 		A version support instance that provides helpers for features active on the current provider connection
     * @param session
     * 		The {@link XASession} instance that is being proxied
     * @param manager
     * 		The owner of this pooled session which receives event from this proxy
     */
    JmsXASessionProxy(JmsConnectionProxyConfiguration configuration,
                          JMSVersionSupport versionSupport,
                          XASession session,
                          JmsSessionProxyManager<JmsXASessionProxy> manager) {
        super(configuration, versionSupport, session, manager);

        this.session = session;
    }

    @Override
    public QueueSession getQueueSession() throws JMSException {
        checkClosed();
        return this;
    }

    @Override
    public TopicSession getTopicSession() throws JMSException {
        checkClosed();
        return this;
    }

    @Override
    public Session getSession() throws JMSException {
        checkClosed();
        return this;
    }

    @Override
    public XAResource getXAResource() {
        checkClosedRuntimeEx();
        return session.getXAResource();
    }

    @Override
    protected JmsXASessionProxy self() {
        return this;
    }
}
