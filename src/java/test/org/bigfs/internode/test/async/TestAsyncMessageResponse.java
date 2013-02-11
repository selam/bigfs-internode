package org.bigfs.internode.test.async;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.bigfs.internode.message.IMessage;
import org.bigfs.internode.message.IVersionedSerializer;

public class TestAsyncMessageResponse  implements IMessage
{
    public static String messageGroup = "TestGroup"; 
    public static int messageType = 101;
    public static TestAsyncMessageResponseSerializer serializer = new TestAsyncMessageResponseSerializer();
    
    
    
    public static class TestAsyncMessageResponseSerializer implements IVersionedSerializer<TestAsyncMessageResponse>
    {

        @Override
        public void serialize(TestAsyncMessageResponse streamHeader,
                DataOutput out, int version) throws IOException
        {
            out.writeUTF("TestAsyncMessageResponse");
            
        }

        @Override
        public TestAsyncMessageResponse deserialize(DataInput in, int version)
                throws IOException
        {
           
            System.out.println(in.readUTF());
            return new TestAsyncMessageResponse();
        }
        
    }

    @Override
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
}
