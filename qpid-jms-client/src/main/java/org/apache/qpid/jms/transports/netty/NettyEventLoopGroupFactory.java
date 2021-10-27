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
import java.util.Optional;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.Future;
import org.apache.qpid.jms.transports.TransportOptions;
import org.apache.qpid.jms.util.QpidJMSThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class NettyEventLoopGroupFactory {

   private NettyEventLoopGroupFactory() {

   }

   public interface Ref<T> extends AutoCloseable {

      T ref();

      @Override
      void close();
   }

   public interface EventLoopGroupRef extends Ref<EventLoopGroup> {

      EventLoopType type();
   }

   private static final Logger LOG = LoggerFactory.getLogger(NettyEventLoopGroupFactory.class);
   private static final AtomicLong SHARED_EVENT_LOOP_GROUP_INSTANCE_SEQUENCE = new AtomicLong(0);
   private static final int SHUTDOWN_TIMEOUT = 50;

   public enum EventLoopType {
      EPOLL, KQUEUE, NIO;

      static EventLoopType valueOf(final TransportOptions transportOptions) {
         final boolean useKQueue = KQueueSupport.isAvailable(transportOptions);
         final boolean useEpoll = EpollSupport.isAvailable(transportOptions);
         if (useKQueue) {
            return KQUEUE;
         }
         if (useEpoll) {
            return EPOLL;
         }
         return NIO;
      }
   }

   private static QpidJMSThreadFactory createSharedQpidJMSThreadFactory(final EventLoopType type, final int threads) {
      return new QpidJMSThreadFactory("SharedNettyEventLoopGroup " + type + ":( threads = " + threads + " - id = " + SHARED_EVENT_LOOP_GROUP_INSTANCE_SEQUENCE.incrementAndGet() + ")", true);
   }

   private static EventLoopGroup createSharedEventLoopGroup(final TransportOptions transportOptions) {
      final EventLoopType eventLoopType = EventLoopType.valueOf(transportOptions);
      return createEventLoopGroup(transportOptions.getSharedEventLoopThreads(), eventLoopType, createSharedQpidJMSThreadFactory(eventLoopType, transportOptions.getSharedEventLoopThreads()));
   }

   private static EventLoopGroup createEventLoopGroup(final int threads,
                                                      final EventLoopType type,
                                                      final ThreadFactory ioThreadFactory) {
      switch (Objects.requireNonNull(type)) {

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

   private static EventLoopGroupRef unsharedGroupWith(final TransportOptions transportOptions,
                                                      final ThreadFactory threadFactory) {
      assert transportOptions.getSharedEventLoopThreads() <= 0;
      final EventLoopType type = EventLoopType.valueOf(transportOptions);
      final EventLoopGroup ref = createEventLoopGroup(1, type, threadFactory);

      return new EventLoopGroupRef() {
         @Override
         public EventLoopType type() {
            return type;
         }

         @Override
         public EventLoopGroup ref() {
            return ref;
         }

         @Override
         public void close() {
            Future<?> fut = ref.shutdownGracefully(0, SHUTDOWN_TIMEOUT, TimeUnit.MILLISECONDS);
            if (!fut.awaitUninterruptibly(2 * SHUTDOWN_TIMEOUT)) {
               LOG.trace("Channel group shutdown failed to complete in allotted time");
            }
         }
      };
   }

   private static final class AtomicCloseableEventLoopGroupRef<T> implements EventLoopGroupRef {

      private final EventLoopGroupRef ref;
      private final AtomicBoolean closed;

      public AtomicCloseableEventLoopGroupRef(final EventLoopGroupRef ref) {
         this.ref = ref;
         this.closed = new AtomicBoolean();
      }

      @Override
      public EventLoopGroup ref() {
         return ref.ref();
      }

      @Override
      public EventLoopType type() {
         return ref.type();
      }

      @Override
      public void close() {
         if (closed.compareAndSet(false, true)) {
            ref.close();
         }
      }
   }

   static Optional<EventLoopGroupRef> sharedExistingGroupWith(final TransportOptions transportOptions) {
      Objects.requireNonNull(transportOptions);
      if (transportOptions.getSharedEventLoopThreads() <= 0) {
         return Optional.empty();
      }
      return Optional.ofNullable(SharedGroupRef.of(transportOptions, false));
   }

   public static EventLoopGroupRef groupWith(final TransportOptions transportOptions,
                                             final ThreadFactory threadfactory) {
      Objects.requireNonNull(transportOptions);
      if (transportOptions.getSharedEventLoopThreads() > 0) {
         return SharedGroupRef.of(transportOptions, true);
      }
      return unsharedGroupWith(transportOptions, threadfactory);
   }

   private static class EventLoopGroupKey {

      private final EventLoopType type;
      private final int eventLoopThreads;

      private EventLoopGroupKey(final EventLoopType type, final int eventLoopThreads) {
         if (eventLoopThreads <= 0) {
            throw new IllegalArgumentException("eventLoopThreads must be > 0");
         }
         this.type = Objects.requireNonNull(type);
         this.eventLoopThreads = eventLoopThreads;
      }

      public EventLoopType type() {
         return type;
      }

      @Override
      public boolean equals(final Object o) {
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

      private static EventLoopGroupKey of(final TransportOptions transportOptions) {
         return new EventLoopGroupKey(EventLoopType.valueOf(transportOptions), transportOptions.getSharedEventLoopThreads());
      }
   }

   private static final class SharedGroupRef implements EventLoopGroupRef {

      private static final Map<EventLoopGroupKey, SharedGroupRef> SHARED_EVENT_LOOP_GROUPS = new HashMap<>();
      private final EventLoopGroupKey key;
      private final EventLoopGroup group;
      private final AtomicInteger refCnt;

      private SharedGroupRef(final EventLoopGroup group, final EventLoopGroupKey key) {
         this.group = Objects.requireNonNull(group);
         this.key = Objects.requireNonNull(key);
         refCnt = new AtomicInteger(1);
      }

      @Override
      public EventLoopType type() {
         return key.type();
      }

      public boolean retain() {
         while (true) {
            final int currValue = refCnt.get();
            if (currValue == 0) {
               return false;
            }
            if (refCnt.compareAndSet(currValue, currValue + 1)) {
               return true;
            }
         }
      }

      @Override
      public EventLoopGroup ref() {
         if (refCnt.get() == 0) {
            throw new IllegalStateException("the event loop group cannot be reused");
         }
         return group;
      }

      @Override
      public void close() {
         while (true) {
            final int currValue = refCnt.get();
            if (currValue == 0) {
               return;
            }
            if (refCnt.compareAndSet(currValue, currValue - 1)) {
               if (currValue == 1) {
                  synchronized (SHARED_EVENT_LOOP_GROUPS) {
                     // SharedGroupRef::of can race with this and replace it
                     SHARED_EVENT_LOOP_GROUPS.remove(key, this);
                  }
                  Future<?> fut = group.shutdownGracefully(0, SHUTDOWN_TIMEOUT, TimeUnit.MILLISECONDS);
                  if (!fut.awaitUninterruptibly(2 * SHUTDOWN_TIMEOUT)) {
                     LOG.trace("Channel group shutdown failed to complete in allotted time");
                  }
               }
               return;
            }
         }
      }

      public static EventLoopGroupRef of(final TransportOptions transportOptions, final boolean canCreate) {
         assert transportOptions.getSharedEventLoopThreads() > 0;
         synchronized (SHARED_EVENT_LOOP_GROUPS) {
            final EventLoopGroupKey key = EventLoopGroupKey.of(transportOptions);
            final SharedGroupRef sharedGroupRef = SHARED_EVENT_LOOP_GROUPS.get(key);
            if (sharedGroupRef != null && sharedGroupRef.retain()) {
               return new AtomicCloseableEventLoopGroupRef(sharedGroupRef);
            }
            if (!canCreate) {
               return null;
            }
            final SharedGroupRef newSharedGroupRef = new SharedGroupRef(createSharedEventLoopGroup(transportOptions), key);
            // this can race with sharedGroupRef::close: who would arrive first, win
            SHARED_EVENT_LOOP_GROUPS.put(key, newSharedGroupRef);
            return new AtomicCloseableEventLoopGroupRef<>(newSharedGroupRef);
         }
      }
   }

}
