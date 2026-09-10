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
package org.messaginghub.pooled.jms.util;

/**
 * Defines a referenced object that is acquired and released which may
 * trigger outcomes when the object becomes fully unreferenced.
 */
public interface Referenced {

    /**
     * Acquires a reference to this referenced object.
     *
     * @return a reference to this reference instance for chaining.
     */
    Referenced acquire();

    /**
     * Returns a reference to this object and if the object has no more
     * references this method returns <code>true</code> other return
     * <code>false</code> to indicate more references remain.
     *
     * @return <code>true</code> if the call to release results in an unreferenced state/
     */
    boolean release();

    /**
     * Check if there are still references to this object.
     *
     * @return <code>true</code> if any outstanding references remain.
     */
    boolean isReferenced();

    /**
     * Check if there are no references to this object.
     *
     * @return <code>true</code> if no outstanding references remain.
     */
    boolean isUnreferenced();

}
