package org.bigfs.internode.test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.bigfs.internode.message.IMessage;
import org.bigfs.internode.message.IVersionedSerializer;

public class Test implements IMessage
{

    public static TestMessageSerializer serializer = new TestMessageSerializer();
    public static TestMessageResponseSerializer responseSerializer = new TestMessageResponseSerializer();

    public static String messageGroup = "TestGroup";
    public static int messageType = 15;
    
    private String test;
    
    public Test(String message){
        this.test = message;
    }
    
    public String getText(){
        return this.test;
    }
    
    @Override
    public String getMessageGroup()
    {
        // TODO Auto-generated method stub
        return Test.messageGroup;
    }

    @Override
    public int getMessageTimeout()
    {
        // TODO Auto-generated method stub
        return 0;
    }

    private static class TestMessageSerializer implements IVersionedSerializer<Test> {

        @Override
        public void serialize(Test t, DataOutput out, int version) throws IOException
        {
            out.writeUTF(t.test);
            
        }

        @Override
        public Test deserialize(DataInput in, int version) throws IOException
        {
            String test = in.readUTF();
            
            return new Test(test);
        }
        
    }
    
    private static class TestMessageResponseSerializer implements IVersionedSerializer<Test> {

        @Override
        public void serialize(Test t, DataOutput out, int version) throws IOException
        {
            out.writeUTF(t.test);
            
        }

        @Override
        public Test deserialize(DataInput in, int version) throws IOException
        {
            String test = in.readUTF();
            
            return new Test(test);
        }
        
    }

    @Override
    public int getMessageType()
    {
        return Test.messageType;
    }

}
