package org.bigfs.internode.events;

import java.net.InetAddress;
import java.util.List;

public class MessagingServiceStartEvent
{
    private final List<InetAddress> addresses;
    
    public MessagingServiceStartEvent(List<InetAddress> addresses)
    {
        this.addresses = addresses;
    }
    
    public List<InetAddress> getAddresses()
    {
        return this.addresses;
    }
}
