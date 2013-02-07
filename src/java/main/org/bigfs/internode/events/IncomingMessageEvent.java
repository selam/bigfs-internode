package org.bigfs.internode.events;

import java.net.InetAddress;

public class IncomingMessageEvent
{
    public final InetAddress from;
    public final int version;
    public final int remoteVersion;
    
    public IncomingMessageEvent(InetAddress from, int guessedOurVersion, int remoteVersion)
    {
        this.from = from;
        this.version = guessedOurVersion;
        this.remoteVersion = remoteVersion;
    }
}
