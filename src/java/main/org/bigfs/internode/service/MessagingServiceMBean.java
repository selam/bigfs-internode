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
package org.bigfs.internode.service;



import java.net.UnknownHostException;
import java.util.Map;

/**
 * MBean exposing MessagingService metrics.
 * - ConnectionPools Messages
 */
public interface MessagingServiceMBean
{
    /**
     * Pending messages on message groups
     */
    public Map<String, Integer> getPendingMessageGroups();
    
    /**
     * Completed messages on message groups
     */
    public Map<String, Long> getCompletedMessageGroups();
    
    /**
     * Dropped messages on message groups
     */
    public Map<String, Long> getDroppedMessageGroups();
    
    /**
     * Pending messages on  Connections
     */
    public Map<String, Integer> getPendingMessages();

    /**
     * Completed messages for Connections
     */
    public Map<String, Long> getCompletedMessages();


    /**
     * dropped message counts for server lifetime
     */
    public Map<String, Long> getDroppedMessages();

    /**
     * Total number of timeouts happened on this node
     */
    public long getTotalTimeouts();

    /**
     * Number of timeouts per host
     */
    public Map<String, Long> getTimeoutsPerHost();

    /**
     * Number of timeouts since last check.
     */
    public long getRecentTotalTimouts();

    /**
     * Number of timeouts since last check per host.
     */
    public Map<String, Long> getRecentTimeoutsPerHost();

    public int getVersion(String address) throws UnknownHostException;
}
