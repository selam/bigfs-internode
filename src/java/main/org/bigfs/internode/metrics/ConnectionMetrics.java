/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bigfs.internode.metrics;

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

import org.bigfs.internode.message.MessageConnectionPool;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;

/**
 * Metrics for {@link MessageConnectionPool}.
 */
public class ConnectionMetrics
{
    public static final String GROUP_NAME = "org.bigfs.internode.metrics";
    public static final String TYPE_NAME = "Connection";

    /** Total number of timeouts happened on this node */
    public static final Meter totalTimeouts = Metrics.newMeter(new MetricName(GROUP_NAME, TYPE_NAME, "TotalTimeouts"), "total timeouts", TimeUnit.SECONDS);
    private static long recentTimeouts;

    public final String address;
   
    /** pending messages */
    public final Gauge<Integer> pendingMessages;
    /** Completed messages */
    public final Gauge<Long> completedMessages;
    /** Dropped messages */
    public final Gauge<Long> droppedMessages;
    
    
    public final Meter timeouts;

    private long recentTimeoutCount;

    /**
     * Create metrics for given connection pool.
     *
     * @param ip IP address to use for metrics label
     * @param messageConnectionPool Connection pool
     */
    public ConnectionMetrics(InetAddress ip, final MessageConnectionPool messageConnectionPool)
    {
        address = ip.getHostAddress();
        pendingMessages = Metrics.newGauge(new MetricName(GROUP_NAME, TYPE_NAME, "CommandPendingTasks", address), new Gauge<Integer>()
        {
            public Integer value()
            {
                return messageConnectionPool.getPendingMessages();
            }
        });
        
        completedMessages = Metrics.newGauge(new MetricName(GROUP_NAME, TYPE_NAME, "CommandCompletedTasks", address), new Gauge<Long>()
        {
            public Long value()
            {
                return messageConnectionPool.getCompletedMesssages();
            }
        });
        droppedMessages = Metrics.newGauge(new MetricName(GROUP_NAME, TYPE_NAME, "CommandDroppedTasks", address), new Gauge<Long>()
        {
            public Long value()
            {
                
                return messageConnectionPool.getDroppedMessages();
            }
        });
        
        timeouts = Metrics.newMeter(new MetricName(GROUP_NAME, TYPE_NAME, "Timeouts", address), "timeouts", TimeUnit.SECONDS);
    }

    public void release()
    {
        Metrics.defaultRegistry().removeMetric(new MetricName(GROUP_NAME, TYPE_NAME, "PendingMessages", address));
        Metrics.defaultRegistry().removeMetric(new MetricName(GROUP_NAME, TYPE_NAME, "CompletedMessages", address));
        Metrics.defaultRegistry().removeMetric(new MetricName(GROUP_NAME, TYPE_NAME, "DroppedMessages", address));
        Metrics.defaultRegistry().removeMetric(new MetricName(GROUP_NAME, TYPE_NAME, "Timeouts", address));
    }

    @Deprecated
    public static long getRecentTotalTimeout()
    {
        long total = totalTimeouts.count();
        long recent = total - recentTimeouts;
        recentTimeouts = total;
        return recent;
    }

    @Deprecated
    public long getRecentTimeout()
    {
        long timeoutCount = timeouts.count();
        long recent = timeoutCount - recentTimeoutCount;
        recentTimeoutCount = timeoutCount;
        return recent;
    }
}
