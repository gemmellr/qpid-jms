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
package org.apache.qpid.jms.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.io.IOException;

import org.junit.jupiter.api.Test;

/**
 *
 */
public class IOExceptionSupportTest {

    @Test
    public void test() {
        new IOExceptionSupport();
    }

    @Test
    public void testPassthrough() {
        IOException cause = new IOException();
        assertSame(cause, IOExceptionSupport.create(cause));
    }

    @Test
    public void testCreateWithMessage() {
        IOException ex = IOExceptionSupport.create(new RuntimeException("error"));
        assertEquals("error", ex.getMessage());
    }

    @Test
    public void testCreateWithoutMessage() {
        IOException ex = IOExceptionSupport.create(new RuntimeException());
        assertNotNull(ex.getMessage());
    }

    @Test
    public void testCreateWithEmptyMessage() {
        IOException ex = IOExceptionSupport.create(new RuntimeException(""));
        assertNotNull(ex.getMessage());
    }
}
