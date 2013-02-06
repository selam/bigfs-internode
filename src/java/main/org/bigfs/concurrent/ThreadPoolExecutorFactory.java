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
package org.bigfs.concurrent;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.bigfs.utils.Helper;


/**
 * This class generates executor services for Messages recieved: each Message requests
 * running on a specific "message group executor service" for concurrency control; hence the Map approach,
 * even though executors are not created dynamically.
 */
public class ThreadPoolExecutorFactory
{

    public static final long KEEPALIVE = 60; // seconds to keep "extra" threads alive for when idle

    public static final int MAX_REPLICATE_ON_WRITE_TASKS = 1024 * Helper.getAvailableProcessors();

        

    public static ThreadPoolExecutor multiThreadedExecutor(String jmxName, String jmxType, int numThreads)
    {
        return new JMXEnabledThreadPoolExecutor(numThreads,
                                                KEEPALIVE,
                                                TimeUnit.SECONDS,
                                                new LinkedBlockingQueue<Runnable>(),
                                                new NamedThreadFactory(jmxName),
                                                jmxType);
    }

    public static ThreadPoolExecutor multiThreadedConfigurableExecutor(String jmxName, String jmxType, int numThreads)
    {
        return new JMXConfigurableThreadPoolExecutor(numThreads,
                                                     KEEPALIVE,
                                                     TimeUnit.SECONDS,
                                                     new LinkedBlockingQueue<Runnable>(),
                                                     new NamedThreadFactory(jmxName),
                                                     jmxType);
    }

    public static ThreadPoolExecutor multiThreadedConfigurableExecutor(String jmxName, String jmxType, int numThreads, int maxTasksBeforeBlock)
    {
        return new JMXConfigurableThreadPoolExecutor(numThreads,
                                                     KEEPALIVE,
                                                     TimeUnit.SECONDS,
                                                     new LinkedBlockingQueue<Runnable>(maxTasksBeforeBlock),
                                                     new NamedThreadFactory(jmxName),
                                                     jmxType);
    }

    
    

    
}
