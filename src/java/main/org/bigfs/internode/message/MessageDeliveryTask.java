package org.bigfs.internode.message;

import org.bigfs.internode.service.MessagingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageDeliveryTask implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(MessageDeliveryTask.class);

    private final MessageIn message;
    private final long constructionTime;

    public MessageDeliveryTask(MessageIn message, long timestamp)
    {
        assert message != null;
        this.message = message;
        constructionTime = timestamp;
    }

    public void run()
    {
        String messageGroup = message.getMessageGroup();       
        int messageType = message.getMessageType();
        if (MessagingService.isDroppableMessage(messageGroup)
            && System.currentTimeMillis() > constructionTime + message.getTimeout())
        {
            return;
        }
        
        CallbackInfo callback = MessagingService.instance().getRegisteredCallback(message.getReplyTo());
        if(callback == null){
            IMessageHandler<? extends IMessage> messageHandler = MessagingService.instance().getMesssageHandler(messageType);
            if (messageHandler == null)
            {                
                logger.debug("Unknown message type {}", messageType);
                return;
            }
            
            messageHandler.processMessage(message); 
        }
        else
        {           
            
           if(callback.callback instanceof IAsyncCallback){
               ((IAsyncCallback) callback.callback).response(message);
           }
           else
           {
               ((IAsyncResult<?>) callback.callback).result(message);
           }
        }
    }
}

