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

import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;

/**
 * A Custom pooled JCA {@link ConnectionFactory} instance.
 */
public class JmsPoolJcaConnectionFactory extends JmsPoolXAConnectionFactory {

    private static final long serialVersionUID = -2470093537159318333L;

    /**
     * Assigned name for JCA factory
     */
    private String name;

    /**
     * Creates the pooling connection factory in the started state but the application must configure
     * a backing {@link ConnectionFactory} before using any method in this object.
     */
    public JmsPoolJcaConnectionFactory() {
        super();
    }

    /**
     * Gets the name that was configured for this JCA {@link ConnectionFactory}
     *
     * @return the name assigned to this JCA ConnectionFactory
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name assigned to this JCA ConnectionFactory
     *
     * @param name The assigned name for this JCA {@link ConnectionFactory}.
     */
    public void setName(String name) {
        this.name = name;
    }

    @Override
    protected JmsPoolSharedJCAConnection createPooledConnection(JmsPoolConnectionConfiguration configuration, Connection connection) {
        return new JmsPoolSharedJCAConnection(configuration, connection, getTransactionManager(), getName());
    }
}
