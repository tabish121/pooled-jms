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

import org.messaginghub.pooled.jms.util.JMSExceptionSupport;

import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSException;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageListener;

/**
 * {@link JMSConsumer} implementation backed by a pooled Connection.
 */
public class JmsPoolJMSConsumer implements JMSConsumer, AutoCloseable {

    private final JmsPoolMessageConsumer consumer;

    JmsPoolJMSConsumer(JmsPoolMessageConsumer consumer) {
        this.consumer = consumer;
    }

    @Override
    public void close() {
        try {
            consumer.close();
        } catch (JMSException e) {
            throw JMSExceptionSupport.createRuntimeException(e);
        }
    }

    //----- MessageConsumer Property Methods ---------------------------------//

    @Override
    public MessageListener getMessageListener() {
        try {
            return consumer.getMessageListener();
        } catch (JMSException e) {
            throw JMSExceptionSupport.createRuntimeException(e);
        }
    }

    @Override
    public String getMessageSelector() {
        try {
            return consumer.getMessageSelector();
        } catch (JMSException e) {
            throw JMSExceptionSupport.createRuntimeException(e);
        }
    }

    @Override
    public void setMessageListener(MessageListener listener) {
        try {
            consumer.setMessageListener(listener);
        } catch (JMSException e) {
            throw JMSExceptionSupport.createRuntimeException(e);
        }
    }

    //----- Receive Methods --------------------------------------------------//

    @Override
    public Message receive() {
        try {
            return consumer.receive();
        } catch (JMSException e) {
            throw JMSExceptionSupport.createRuntimeException(e);
        }
    }

    @Override
    public Message receive(long timeout) {
        try {
            return consumer.receive(timeout);
        } catch (JMSException e) {
            throw JMSExceptionSupport.createRuntimeException(e);
        }
    }

    @Override
    public Message receiveNoWait() {
        try {
            return consumer.receiveNoWait();
        } catch (JMSException e) {
            throw JMSExceptionSupport.createRuntimeException(e);
        }
    }

    @Override
    public <T> T receiveBody(Class<T> desired) {
        throw new JMSRuntimeException("Pooled JMSConsumer does not support receiveBody");
    }

    @Override
    public <T> T receiveBody(Class<T> desired, long timeout) {
        throw new JMSRuntimeException("Pooled JMSConsumer does not support receiveBody");
    }

    @Override
    public <T> T receiveBodyNoWait(Class<T> desired) {
        throw new JMSRuntimeException("Pooled JMSConsumer does not support receiveBody");
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " { " + consumer + " }";
    }

    /**
     * Gets the JMS {@link MessageConsumer} that backs this {@link JMSConsumer} instance. This is
     * primarily meant as a test point and application logic should not depend on this method.
     *
     * @return the {@link MessageConsumer} that backs this {@link JMSConsumer}
     *
     * @throws JMSRuntimeException if an error occurs while accessing the backing consumer.
     */
    public MessageConsumer getMessageProducer() throws JMSRuntimeException {
        try {
            return consumer.getMessageConsumer();
        } catch (JMSException jmsex) {
            throw JMSExceptionSupport.createRuntimeException(jmsex);
        }
    }
}
