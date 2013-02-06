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

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.Map;

import org.bigfs.internode.configuration.MessagingConfiguration;
import org.bigfs.internode.serialization.InetAddressSerialization;

public class MessageOut<T extends IMessage> {

	
	public final String messageGroup;
    public final InetAddress from;    
    public final T payload;
    public final IVersionedSerializer<T> serializer;
    public final Map<String, byte[]> parameters;
    
	public MessageOut(String messageGroup, T payload, IVersionedSerializer<T> serializer)
    {
        this(messageGroup,
             payload,
             serializer,
             Collections.<String, byte[]>emptyMap());
    }

	private MessageOut(String messageGroup, T payload, IVersionedSerializer<T> serializer, Map<String, byte[]> parameters)
    {
        this(MessagingConfiguration.getListenAddress(), messageGroup, payload, serializer, parameters);
    }
    
	public MessageOut(InetAddress from, String messageGroup, T payload, IVersionedSerializer<T> serializer, Map<String, byte[]> parameters)
    {
        this.from = from;
        this.messageGroup = messageGroup;
        this.payload = payload;
        this.serializer = serializer;
        this.parameters = parameters;
    }
	
	
	public MessageOut(String group){
		
		this(group, null, null);
	}
		
	
	public String getMessageGroup(){		
		return this.payload.getMessageGroup();
	}
	
	public int getMessageTimeout(){		
		return this.payload.getMessageTimeout();
	}
	
	public int getMessageType(){        
        return this.payload.getMessageType();
    }
	
	public void serialize(DataOutputStream out, int version) throws IOException
    {
        InetAddressSerialization.serialize(from, out);

        out.writeInt(this.getMessageType());
        out.writeUTF(this.getMessageGroup());
        
        out.writeInt(parameters.size());
        for (Map.Entry<String, byte[]> entry : parameters.entrySet())
        {
            out.writeUTF(entry.getKey());
            out.writeInt(entry.getValue().length);
            out.write(entry.getValue());
        }

        if (payload != null)
            serializer.serialize(payload, out, version);
    }
}
