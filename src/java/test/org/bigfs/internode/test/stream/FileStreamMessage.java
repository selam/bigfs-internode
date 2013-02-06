package org.bigfs.internode.test.stream;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

import org.bigfs.internode.configuration.MessagingConfiguration;
import org.bigfs.internode.service.MessagingService;
import org.bigfs.internode.streaming.AFileStreamTask;
import org.bigfs.internode.streaming.IStreamHeader;
import org.bigfs.utils.Helper;

public class FileStreamMessage extends AFileStreamTask<IStreamHeader>
{
    private int MAX_CONNECT_ATTEMPTS = 4;
    
    private Socket socket;
    private OutputStream out;
    private InputStream in;
    
    public void connect() throws IOException 
    {
        int attempts = 0;
        while (true)
        {
            try
            {
                socket = MessagingService.instance().getConnectionPool(to).getSocket();
                socket.setSoTimeout(MessagingConfiguration.getStreamingSocketTimeout());
                out = socket.getOutputStream();
                in = new DataInputStream(socket.getInputStream());
                break;
            }
            catch(IOException e)
            {
                if (++attempts >= MAX_CONNECT_ATTEMPTS) 
                {
                    throw e;
                }
                long waitms = MessagingConfiguration.getRpcTimeout() * (long)Math.pow(2, attempts);
                
                try
                {
                    Thread.sleep(waitms);
                }
                catch (InterruptedException wtf)
                {
                    throw new RuntimeException(wtf);
                }
            }
        }      
    }

    public void stream() throws IOException
    {
        ByteBuffer streamHeader = MessagingService.getStreamHeader(header,  MessagingService.instance().getRemoteMessagingVersion(to));
        out.write(Helper.getArray(streamHeader));       
        out.write("Write streaming data".getBytes());
        out.flush();
    }
    
    @Override
    public void run()
    {
        try {
            System.out.println("Connect and start write");
            connect();
            stream();
        }
        catch(IOException e) 
        {
            
        }
    }

}
