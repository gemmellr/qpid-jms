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
package org.apache.qpid.jms.transports.netty;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.Future;

import org.apache.qpid.jms.util.QpidJMSThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class NettyEventLoopGroupFactory {

    private static final Logger LOG = LoggerFactory.getLogger(NettyEventLoopGroupFactory.class);
    private static final AtomicLong SHARED_EVENT_LOOP_GROUP_INSTANCE_SEQUENCE = new AtomicLong(0);
    private static final int SHUTDOWN_TIMEOUT = 50;

    private static final Map<EventLoopGroupKey, EventLoopGroupHolder> SHARED_EVENT_LOOP_GROUPS = new HashMap<>();

    private NettyEventLoopGroupFactory() {
        // No instances
    }

    public static EventLoopGroupRef unsharedGroup(final EventLoopType type, final ThreadFactory threadFactory) {
        Objects.requireNonNull(type);
        final EventLoopGroup unsharedGroup = createEventLoopGroup(1, type, threadFactory);

        return new EventLoopGroupRef() {
            @Override
            public EventLoopGroup group() {
                return unsharedGroup;
            }

            @Override
            public void close() {
                shutdownEventLoopGroup(unsharedGroup);
            }
        };
    }

    public static EventLoopGroupRef sharedGroup(final EventLoopType type, final int threads) {
        Objects.requireNonNull(type);
        if (threads <= 0) {
            throw new IllegalArgumentException("shared event loop threads value must be > 0");
        }

        final EventLoopGroupKey key = new EventLoopGroupKey(type, threads);

        synchronized (SHARED_EVENT_LOOP_GROUPS) {
            EventLoopGroupHolder groupHolder = SHARED_EVENT_LOOP_GROUPS.get(key);
            if (groupHolder == null) {
                groupHolder = new EventLoopGroupHolder(createSharedEventLoopGroup(type, threads), key);

                SHARED_EVENT_LOOP_GROUPS.put(key, groupHolder);
            } else {
                groupHolder.incRef();
            }

            return new SharedEventLoopGroupRef(groupHolder);
        }
    }

    private static void sharedGroupRefClosed(EventLoopGroupHolder holder) {
        boolean shutdown = false;
        synchronized (SHARED_EVENT_LOOP_GROUPS) {
            int remaining = holder.decRef();
            if(remaining == 0) {
                SHARED_EVENT_LOOP_GROUPS.remove(holder.key());
                shutdown = true;
            }
        }

        if(shutdown) {
            shutdownEventLoopGroup(holder.group());
        }
    }

    private static EventLoopGroup createEventLoopGroup(final int threads, final EventLoopType type, final ThreadFactory ioThreadFactory) {
        switch (type) {
        case EPOLL:
            LOG.trace("Netty Transport using Epoll mode");
            return EpollSupport.createGroup(threads, ioThreadFactory);
        case KQUEUE:
            LOG.trace("Netty Transport using KQueue mode");
            return KQueueSupport.createGroup(threads, ioThreadFactory);
        case NIO:
            LOG.trace("Netty Transport using Nio mode");
            return new NioEventLoopGroup(threads, ioThreadFactory);
        default:
            throw new AssertionError("unexpected type: " + type);
        }
    }

    private static void shutdownEventLoopGroup(final EventLoopGroup group) {
        Future<?> fut = group.shutdownGracefully(0, SHUTDOWN_TIMEOUT, TimeUnit.MILLISECONDS);
        if (!fut.awaitUninterruptibly(2 * SHUTDOWN_TIMEOUT)) {
            LOG.trace("Channel group shutdown failed to complete in allotted time");
        }
    }

    private static EventLoopGroup createSharedEventLoopGroup(final EventLoopType type, final int threads) {
        //TODO: ensure each thread has unique name.
        ThreadFactory tf = new QpidJMSThreadFactory("SharedNettyEventLoopGroup" + type + ":( threads = " + threads + " - id = " + SHARED_EVENT_LOOP_GROUP_INSTANCE_SEQUENCE.incrementAndGet() + ")", true);

        return createEventLoopGroup(threads, type, tf);
    }

    private static final class SharedEventLoopGroupRef implements EventLoopGroupRef {
        private final EventLoopGroupHolder sharedGroupHolder;
        private final AtomicBoolean closed = new AtomicBoolean();

        public SharedEventLoopGroupRef(final EventLoopGroupHolder sharedGroupHolder) {
            this.sharedGroupHolder = Objects.requireNonNull(sharedGroupHolder);
        }

        @Override
        public EventLoopGroup group() {
            if (closed.get()) {
                throw new IllegalStateException("Group reference is already closed");
            }

            return sharedGroupHolder.group();
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                sharedGroupRefClosed(sharedGroupHolder);
            }
        }
    }

    private static class EventLoopGroupKey {
        private final EventLoopType type;
        private final int eventLoopThreads;

        private EventLoopGroupKey(final EventLoopType type, final int eventLoopThreads) {
            this.type = type;
            this.eventLoopThreads = eventLoopThreads;
        }

        @Override
        public boolean equals(final Object o) {
            //TODO: add braces
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            final EventLoopGroupKey that = (EventLoopGroupKey) o;
            if (eventLoopThreads != that.eventLoopThreads)
                return false;
            return type == that.type;
        }

        @Override
        public int hashCode() {
            int result = type != null ? type.hashCode() : 0;
            result = 31 * result + eventLoopThreads;
            return result;
        }
    }

    private static final class EventLoopGroupHolder {
        private final EventLoopGroup group;
        private final EventLoopGroupKey key;
        private int refCnt = 1;

        private EventLoopGroupHolder(final EventLoopGroup sharedGroup, final EventLoopGroupKey key) {
            this.group = Objects.requireNonNull(sharedGroup);
            this.key = Objects.requireNonNull(key);
        }

        public EventLoopGroup group() {
            return group;
        }

        public EventLoopGroupKey key() {
            return key;
        }

        public int incRef() {
            return refCnt++;
        }

        public int decRef() {
            return --refCnt;
        }
    }
}