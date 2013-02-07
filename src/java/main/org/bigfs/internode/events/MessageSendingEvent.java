package org.bigfs.internode.events;

import java.net.InetAddress;

public class MessageSendingEvent
{
    public final InetAddress to;
    public final int remoteVersion;
    public MessageSendingEvent(InetAddress to, int remoteVersion)
    {
        this.to = to;
        this.remoteVersion = remoteVersion;
    }
}
