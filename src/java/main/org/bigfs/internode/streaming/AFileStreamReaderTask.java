package org.bigfs.internode.streaming;

import java.net.Socket;

public abstract class AFileStreamReaderTask
{
    protected Socket socket;
    protected int guessedOurVersion;
    
    public void initialize(Socket socket, int guessedOurVersion)
    {
        this.socket = socket;
        this.guessedOurVersion = guessedOurVersion;
    }
    
    abstract public void read();
}
