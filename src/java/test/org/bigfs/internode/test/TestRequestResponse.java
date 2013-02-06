package org.bigfs.internode.test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.bigfs.internode.message.IMessage;
import org.bigfs.internode.message.IVersionedSerializer;

public class TestRequestResponse implements IMessage
{

    public static TestMessageSerializer serializer = new TestMessageSerializer();

    public static String messageGroup = "TestGroup";
    public static int messageType = 1;
    
    private String test;
    
    public TestRequestResponse(String message){
        this.test = message;
    }
    
    public String getText(){
        return this.test;
    }
    
    @Override
    public String getMessageGroup()
    {
        // TODO Auto-generated method stub
        return TestRequestResponse.messageGroup;
    }

    @Override
    public int getMessageTimeout()
    {
        // TODO Auto-generated method stub
        return 0;
    }

    private static class TestMessageSerializer implements IVersionedSerializer<TestRequestResponse> {

        @Override
        public void serialize(TestRequestResponse t, DataOutput out, int version) throws IOException
        {
            out.writeUTF(t.test);
            
        }

        @Override
        public TestRequestResponse deserialize(DataInput in, int version) throws IOException
        {
            String test = in.readUTF();
            
            return new TestRequestResponse(test);
        }
        
    }
    
    private static class TestMessageResponseSerializer implements IVersionedSerializer<TestRequestResponse> {

        @Override
        public void serialize(TestRequestResponse t, DataOutput out, int version) throws IOException
        {
            out.writeUTF(t.test);
            
        }

        @Override
        public TestRequestResponse deserialize(DataInput in, int version) throws IOException
        {
            String test = in.readUTF();
            
            return new TestRequestResponse(test);
        }
        
    }

    @Override
    public int getMessageType()
    {
        return TestRequestResponse.messageType;
    }

}
