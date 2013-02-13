package org.bigfs.internode.service;

import org.bigfs.internode.events.CallbackExpireEvent;
import org.bigfs.internode.message.CallbackInfo;
import org.bigfs.internode.metrics.ConnectionMetrics;

import com.google.common.eventbus.AllowConcurrentEvents;
import com.google.common.eventbus.Subscribe;

public class MessagingServiceEventHandler
{
    @Subscribe
    @AllowConcurrentEvents
    public void CallbackExpireEventHandler(CallbackExpireEvent event)
    {       
        CallbackInfo callback = event.getCallbackInfo();
        ConnectionMetrics.totalTimeouts.mark();
        MessagingService.instance().getConnectionPool(callback.target).incrementTimeout();        
    }
}
