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

import javax.transaction.xa.XAResource;

import org.apache.geronimo.transaction.manager.WrapperNamedXAResource;
import org.messaginghub.pooled.jms.internal.JmsPoolXAConnectionProxy;

import jakarta.jms.JMSException;
import jakarta.jms.XAConnection;
import jakarta.transaction.TransactionManager;

public class JmsPoolJcaConnection extends JmsPoolXAConnection implements XAConnection {

    private final String name;

    JmsPoolJcaConnection(JmsPoolXAConnectionProxy connection, TransactionManager transactionManager, String name) {
        super(connection, transactionManager);

        this.name = name;
    }

    /**
     * Gets the name that was configured for this JCA {@link XAConnection}
     *
     * @return the name assigned to by the JCA XAConnectionFactory
     */
    public String getName() {
        return name;
    }

    @Override
    protected XAResource createXaResource(JmsPoolXASession session) throws JMSException {
        XAResource xares = session.getXAResource();

        if (name != null) {
            xares = new WrapperNamedXAResource(xares, name);
        }

        return xares;
    }
}
