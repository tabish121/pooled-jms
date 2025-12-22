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

/**
 * A cache key for the session details used to locate PooledSession instances.
 */
final class JmsPoolSessionKey {

    private final boolean transacted;
    private final int ackMode;
    private final int hashCode;

    public JmsPoolSessionKey(boolean transacted, int ackMode) {
        this.transacted = transacted;
        this.ackMode = ackMode;
        this.hashCode = computeHash(transacted, ackMode);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    public boolean isTransacted() {
        return transacted;
    }

    public int getAckMode() {
        return ackMode;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }

        final JmsPoolSessionKey other = (JmsPoolSessionKey) obj;

        if (hashCode != other.hashCode) {
            return false;
        }
        if (ackMode != other.ackMode) {
            return false;
        }
        if (transacted != other.transacted) {
            return false;
        }

        return true;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " { " + ackMode + ", " + transacted + " }";
    }

    private static int computeHash(boolean transacted, int ackMode) {
        final int prime = 31;
        int result = 1;
        result = prime * result + ackMode;
        result = prime * result + (transacted ? 1231 : 1237);
        return result;
    }
}
