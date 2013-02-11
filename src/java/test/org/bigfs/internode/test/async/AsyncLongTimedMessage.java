package org.bigfs.internode.test.async;

import org.bigfs.internode.message.IAsyncCallback;
import org.bigfs.internode.message.MessageIn;

public class AsyncLongTimedMessage implements IAsyncCallback
{

    protected MessageIn message;
    
    @Override
    public boolean isLatencyForSnitch()
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void response(MessageIn msg)
    {
        this.message = msg;
        
        try
        {
            Thread.sleep(2000L);
        }
        catch (InterruptedException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }        
    }

}
