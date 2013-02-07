package org.bigfs.internode.events;

import org.bigfs.internode.message.MessageConnection;

public class DiscardedMessageEvent
{
    MessageConnection.QueuedMessage message;
    public DiscardedMessageEvent(MessageConnection.QueuedMessage message){
        this.message = message;
    }
}
