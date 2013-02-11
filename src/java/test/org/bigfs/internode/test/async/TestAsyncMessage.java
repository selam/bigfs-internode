package org.bigfs.internode.test.async;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.bigfs.internode.message.IMessage;
import org.bigfs.internode.message.IVersionedSerializer;

public class TestAsyncMessage implements IMessage
{
    public static String messageGroup = "TestGroup"; 
    public static int messageType = 1;
    public static TestAsyncMessageSerializer serializer = new TestAsyncMessageSerializer();
    public String message;
    
    public TestAsyncMessage(String message) {
        this.message=message;
    }
    
    public String getMessageGroup()
    {
        // TODO Auto-generated method stub
        return messageGroup;
    }

    @Override
    public int getMessageTimeout()
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMessageType()
    {
        // TODO Auto-generated method stub
        return messageType;
    }
    
    public static class TestAsyncMessageSerializer implements IVersionedSerializer<TestAsyncMessage>
    {

        @Override
        public void serialize(TestAsyncMessage message,
                DataOutput out, int version) throws IOException
        {
            out.writeUTF(message.message);
            
        }

        @Override
        public TestAsyncMessage deserialize(DataInput in, int version)
                throws IOException
        {
            String message = in.readUTF();
            return new TestAsyncMessage(message);
        }
        
    }
}
