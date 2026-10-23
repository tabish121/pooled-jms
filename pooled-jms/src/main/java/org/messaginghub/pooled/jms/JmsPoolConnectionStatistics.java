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
 * Interface for the statistics API exposed for a JMS pooled connection and the resources
 *  that it manages.
 */
public interface JmsPoolConnectionStatistics {

    /**
     * Returns the total number of sessions that are currently in the session pool
     * managed by a pooled JMS Connection instance.
     *
     * The pool is actually a collection of sub-pools divided by the transaction mode
     * and acknowledgement mode of the sessions created from this pooled connection.
     * The returned value reflects the count of all session in all sub-pools.
     *
     * @return the current number of session in the session pool
     */
    int getSessionCount();

    /**
     * Returns the total number of currently loaned sessions that are currently in the
     * session pool managed by a pooled JMS Connection instance.
     *
     * The pool is actually a collection of sub-pools divided by the transaction mode
     * and acknowledgement mode of the sessions created from this pooled connection. The
     * number returned reflects all active sessions in all sub-pools.
     *
     * @return the current number of active session in the session pool
     */
    int getActiveSessionCount();

    /**
     * Returns the total number of not currently idle sessions that are currently in the
     * session pool managed by a pooled JMS Connection instance, idle sessions are those that
     * are not currently loaned to any user.
     *
     * The pool is actually a collection of sub-pools divided by the transaction mode
     * and acknowledgement mode of the sessions created from this pooled connection. The
     * number returned reflects all idle sessions in all sub-pools.
     *
     * @return the current number of idle session in the session pool
     */
    int getIdleSessionCount();

    /**
     * Returns the total number of sessions that are currently in the session pool
     * managed by a pooled JMS Connection instance that have the given transacted and
     * session acknowledgement mode values.
     *
     * The pool is actually a collection of sub-pools divided by the transaction mode
     * and acknowledgement mode of the sessions created from this pooled connection.
     * The returned value reflects the count of all session in the given sub-pool that
     * manages session with the given transacted and acknowledgement mode combination..
     *
     * @param transacted
     * 		Filter for sessions with only the given transacted state
     * @param acknowledgementMode
     * 		Filter for sessions with only the given acknowledgement mode
     *
     * @return the current number of sessions in the filtered session pool
     */
    int getSessionCount(boolean transacted, int acknowledgementMode);

    /**
     * Returns the total number of active sessions that are currently in the session pool
     * managed by a pooled JMS Connection instance that have the given transacted and
     * session acknowledgement mode values.
     *
     * The pool is actually a collection of sub-pools divided by the transaction mode
     * and acknowledgement mode of the sessions created from this pooled connection.
     * The returned value reflects the count of all sessions currently loaned out by the
     * given sub-pool that manages session with the given transacted and acknowledgement
     * mode combination..
     *
     * @param transacted
     * 		Filter for sessions with only the given transacted state
     * @param acknowledgementMode
     * 		Filter for sessions with only the given acknowledgement mode
     *
     * @return the current number of active sessions in the filtered session pool
     */
    int getActiveSessionCount(boolean transacted, int acknowledgementMode);

    /**
     * Returns the total number of idle sessions that are currently in the session pool
     * managed by a pooled JMS Connection instance that have the given transacted and
     * session acknowledgement mode values.
     *
     * The pool is actually a collection of sub-pools divided by the transaction mode
     * and acknowledgement mode of the sessions created from this pooled connection.
     * The returned value reflects the count of all session currently idle in the given
     * sub-pool that manages session with the given transacted and acknowledgement mode
     * combination..
     *
     * @param transacted
     * 		Filter for sessions with only the given transacted state
     * @param acknowledgementMode
     * 		Filter for sessions with only the given acknowledgement mode
     *
     * @return the current number of idle sessions in the filtered session pool
     */
    int getIdleSessionCount(boolean transacted, int acknowledgementMode);

}
