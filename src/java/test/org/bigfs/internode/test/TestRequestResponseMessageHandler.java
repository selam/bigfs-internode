package org.bigfs.internode.test;

import org.bigfs.internode.message.IMessageHandler;
import org.bigfs.internode.message.MessageIn;
import org.bigfs.internode.message.MessageOut;
import org.bigfs.internode.service.MessagingService;


public class TestRequestResponseMessageHandler implements IMessageHandler<TestRequestResponse>
{

    @Override
    public void processMessage(MessageIn<TestRequestResponse> message)
    {
        Test t = new Test("deneme response");
     
        MessageOut<Test> m = new MessageOut<Test>(t.getMessageGroup(), t, Test.serializer);
        
        MessagingService.instance().sendReply(m, message.getMessageId(), message.from);      
    }





	
    
   

}
