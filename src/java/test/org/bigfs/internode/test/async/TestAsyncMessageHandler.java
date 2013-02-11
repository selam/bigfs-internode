package org.bigfs.internode.test.async;

import org.bigfs.internode.message.IMessageHandler;
import org.bigfs.internode.message.MessageIn;
import org.bigfs.internode.message.MessageOut;
import org.bigfs.internode.service.MessagingService;

public class TestAsyncMessageHandler implements IMessageHandler<TestAsyncMessage>
{

    @Override
    public void processMessage(MessageIn<TestAsyncMessage> message)
    {
        try
        {
            Thread.sleep(5000L);
            // TODO Auto-generated method stub
            TestAsyncMessageResponse t = new TestAsyncMessageResponse();
            
            MessageOut<TestAsyncMessageResponse> m = new MessageOut<TestAsyncMessageResponse>(t.getMessageGroup(), t, TestAsyncMessageResponse.serializer);
            MessagingService.instance().sendReply(m, message.getMessageId(), message.from);
        }
        catch (InterruptedException e)
        {
            System.out.println(e);
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
    }

}
