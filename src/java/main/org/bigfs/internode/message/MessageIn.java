package org.bigfs.internode.message;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.Map;

import org.bigfs.internode.serialization.InetAddressSerialization;
import org.bigfs.internode.service.MessagingService;

import com.google.common.collect.ImmutableMap;

public class MessageIn<T extends IMessage>
{
    public final InetAddress from;
    public final T payload;
    public final Map<String, byte[]> parameters;
    public final int messageType;
    public final String messageGroup;
    public final int version;
    public final String id;
    public final String replyTo;
    
    private MessageIn(InetAddress from, T payload, Map<String, byte[]> parameters, int messageType, String messageGroup, int version, String id, String replyTo)
    {
        this.from = from;
        this.payload = payload;
        this.parameters = parameters;
        this.messageType = messageType;
        this.version = version;
        this.messageGroup = messageGroup;
        this.id = id;
        this.replyTo = replyTo;
    }

    
    public static <T extends IMessage> MessageIn<T> create(InetAddress from, T payload, Map<String, byte[]> parameters, int messageType, String messageGroup, int version, String id, String replyTo)
    {
        return new MessageIn<T>(from, payload, parameters, messageType, messageGroup, version, id, replyTo);
    }
    
    public static <T2 extends IMessage> MessageIn<T2> read(DataInputStream input, int version, String id, String replyTo) throws IOException
    {        
        InetAddress from = InetAddressSerialization.deserialize(input); 
        int messageType = input.readInt();
        String messageGroup = input.readUTF();
        
        int parameterCount = input.readInt();
        
        Map<String, byte[]> parameters;        
        if (parameterCount == 0)
        {
            parameters = Collections.emptyMap();
        }
        else
        {
            ImmutableMap.Builder<String, byte[]> builder = ImmutableMap.builder();
            for (int i = 0; i < parameterCount; i++)
            {
                String key = input.readUTF();
                byte[] value = new byte[input.readInt()];
                input.readFully(value);
                builder.put(key, value);
            }
            parameters = builder.build();
        }
        
        IVersionedSerializer<T2> serializer = (IVersionedSerializer<T2>) MessagingService.getMessageSerializer(messageType);
        
        
        if (serializer == null)            
            return MessageIn.create(from, null, parameters, messageType, messageGroup, version, id, replyTo);
        
        T2 payload = serializer.deserialize(input, version);
        
       
        return MessageIn.create(from, payload, parameters, messageType, messageGroup, version, id, replyTo);
    }
    
    public long getTimeout()
    {
        return payload.getMessageTimeout();
    }
    

    public String getMessageGroup()
    {
       return payload.getMessageGroup();
    }
    
    public int getMessageType()
    {
       return payload.getMessageType();
    }

    public T getPayload(){
    	return payload;
    }
    
    public String getReplyTo(){
        return replyTo;
    }
    
    public String getMessageId(){
        return id;
    }
}
