package org.bigfs.internode.test;

import org.bigfs.internode.message.IMessageHandler;
import org.bigfs.internode.message.MessageIn;


public class TestMessageHandler implements IMessageHandler<Test>
{

    @Override
    public void processMessage(MessageIn<Test> message)
    {
        System.out.println(message.getPayload().getText());        
    }
}
