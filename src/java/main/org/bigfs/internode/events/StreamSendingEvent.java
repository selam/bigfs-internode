package org.bigfs.internode.events;

import java.net.InetAddress;

import org.bigfs.internode.streaming.IStreamHeader;

public class StreamSendingEvent
{
    public final IStreamHeader header;
    public final InetAddress to;
    
    public StreamSendingEvent(InetAddress to, IStreamHeader streamHeader)
    {
        this.to = to;
        this.header = streamHeader;
    }
}
