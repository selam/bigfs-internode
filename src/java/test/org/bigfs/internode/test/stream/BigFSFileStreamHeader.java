package org.bigfs.internode.test.stream;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.bigfs.internode.message.IVersionedSerializer;
import org.bigfs.internode.streaming.IStreamHeader;

public class BigFSFileStreamHeader implements IStreamHeader 
{
    /**
     * Write your own file message structure, serialize and deserialize
     */
    public static BigFSFileStreamHeaderSerializer serializer = new BigFSFileStreamHeaderSerializer();
    public static int messageType = -1;
    @Override
    public String getMessageGroup()
    {
        return null;
    }

    @Override
    public int getMessageTimeout()
    {
        return 0;
    }

    @Override
    public int getMessageType()
    {
        return 0;
    }

    @Override
    public BigFSFileStreamHeaderSerializer getSerializer()
    {
        return serializer;
    }

    @Override
    public boolean isCompressed()
    {
        return false;
    }

    @Override
    public int fileCount()
    {
        return 0;
    }
    
    static class BigFSFileStreamHeaderSerializer implements IVersionedSerializer<BigFSFileStreamHeader>
    {

        @Override
        public void serialize(BigFSFileStreamHeader streamHeader,
                DataOutput out, int version) throws IOException
        {
            // TODO Auto-generated method stub
            
        }

        @Override
        public BigFSFileStreamHeader deserialize(DataInput in, int version)
                throws IOException
        {
            // TODO Auto-generated method stub
            return null;
        }

   
        
    }
}
