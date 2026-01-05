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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class ReferenceCountTest {

    @Test
    void testCreate() {
        final ReferenceCount count = new ReferenceCount();

        assertEquals(0, count.getCount());
        assertTrue(count.isUnreferenced());
    }

    @Test
    void testCreateWithFloor() {
        final ReferenceCount count = new ReferenceCount(1);

        assertEquals(1, count.getCount());
        assertEquals(1, count.decrementAndGet());
        assertTrue(count.isUnreferenced());
    }

    @Test
    void testCreateWithFloorPostGet() {
        final ReferenceCount count = new ReferenceCount(1);

        assertEquals(1, count.getCount());
        assertEquals(1, count.getAndDecrement());
        assertTrue(count.isUnreferenced());
    }

    @Test
    void testCreateWithInvalidFloor() {
        assertThrows(IllegalArgumentException.class, () -> new ReferenceCount(-1));
    }

    @Test
    void testIncrement() {
        final ReferenceCount count = new ReferenceCount();

        assertTrue(count.isUnreferenced());
        assertEquals(0, count.getCount());
        assertEquals(1, count.incrementAndGet());
        assertFalse(count.isUnreferenced());
        assertEquals(2, count.incrementAndGet());
        assertFalse(count.isUnreferenced());
    }

    @Test
    void testGetAndIncrement() {
        final ReferenceCount count = new ReferenceCount();

        assertTrue(count.isUnreferenced());
        assertEquals(0, count.getCount());
        assertEquals(0, count.getAndIncrement());
        assertFalse(count.isUnreferenced());
        assertEquals(1, count.getAndIncrement());
        assertFalse(count.isUnreferenced());
    }

    @Test
    void testGetAndDecrement() {
        final ReferenceCount count = new ReferenceCount();

        assertTrue(count.isUnreferenced());
        assertEquals(0, count.getCount());
        assertEquals(1, count.incrementAndGet());
        assertFalse(count.isUnreferenced());
        assertEquals(2, count.incrementAndGet());
        assertFalse(count.isUnreferenced());
        assertEquals(2, count.getAndDecrement());
        assertEquals(1, count.getAndDecrement());
        assertTrue(count.isUnreferenced());
    }

    @Test
    void testIncrementPastMaxInt() {
        final ReferenceCount count = new ReferenceCount(Integer.MAX_VALUE - 1);

        assertEquals(Integer.MAX_VALUE - 1, count.getCount());
        assertEquals(Integer.MAX_VALUE, count.incrementAndGet());
        assertEquals(Integer.MAX_VALUE, count.incrementAndGet());
        assertEquals(Integer.MAX_VALUE - 1, count.decrementAndGet());
    }

    @Test
    void testGetAndIncrementPastMaxInt() {
        final ReferenceCount count = new ReferenceCount(Integer.MAX_VALUE - 1);

        assertEquals(Integer.MAX_VALUE - 1, count.getCount());
        assertEquals(Integer.MAX_VALUE - 1, count.getAndIncrement());
        assertEquals(Integer.MAX_VALUE, count.getAndIncrement());
        assertEquals(Integer.MAX_VALUE - 1, count.decrementAndGet());
    }

    @Test
    void testDecrementPastFloor() {
        final ReferenceCount count = new ReferenceCount(10);

        assertEquals(10, count.getCount());
        assertEquals(11, count.incrementAndGet());
        assertEquals(12, count.incrementAndGet());
        assertEquals(11, count.decrementAndGet());
        assertEquals(10, count.decrementAndGet());
        assertEquals(10, count.decrementAndGet());
    }
}
