package org.bigfs.internode.events;

import java.net.InetAddress;

public class CloseMessageConnectionEvent
{
    public final InetAddress to;
    public final boolean destroyThread;
    public CloseMessageConnectionEvent(InetAddress to, boolean destroyThread)
    {
        this.to = to;
        this.destroyThread = destroyThread;
    }
}
