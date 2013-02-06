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

package org.bigfs.internode.message;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SocketChannel;

import org.bigfs.internode.configuration.MessagingConfiguration;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class ConnectionPool {
	
	private static final Logger logger = LoggerFactory.getLogger(ConnectionPool.class);
	
	private final InetAddress id;
	
	private final NonBlockingHashMap<String, MessageConnection> connections = new NonBlockingHashMap<String, MessageConnection>();
	
	
	public ConnectionPool(InetAddress id) 
	{
		this.id = id;		
	}
	
	public MessageConnection getConnection(MessageOut<?> m)
	{
		 String connType = m.getMessageGroup();
		 
		 if(!this.connections.contains(connType))
		 {
			 logger.debug("create new connection to {} for {}", this.id, connType);
			 MessageConnection con = new MessageConnection(this);
			 con.start();
			 this.connections.put(connType, con);
		 }
		 
		 return this.connections.get(connType);
	}
	
	public Socket getSocket() throws IOException
    {       
       return  SocketChannel.open(new InetSocketAddress(id, MessagingConfiguration.getPort())).socket();           
    }
	
	public InetAddress getRemoteAddress(){
		return this.id;
	}
	
	public void close()
	{
		for(MessageConnection con: connections.values())
		{
			con.close(true);
		}
	}
}
