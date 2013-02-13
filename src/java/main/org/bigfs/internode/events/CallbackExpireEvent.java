package org.bigfs.internode.events;

import org.bigfs.internode.message.CallbackInfo;

public class CallbackExpireEvent
{
    protected CallbackInfo callback;
    protected long timeout;
    
    public CallbackExpireEvent(CallbackInfo callbackInfo, long timeout)
    {
        this.callback = callbackInfo;
        this.timeout = timeout; 
    }
    
    public CallbackInfo getCallbackInfo()
    {
        return callback;
    }
    
    public long getTimeout()
    {
        return timeout;
    }
}
