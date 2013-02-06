package org.bigfs.internode.message;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;

import org.bigfs.internode.configuration.MessagingConfiguration;
import org.bigfs.internode.serialization.InetAddressSerialization;
import org.bigfs.internode.service.MessagingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyInputStream;

public class MessageReader extends Thread
{
   private final Socket socket;
   private InetAddress from;
   
   private static final Logger logger = LoggerFactory.getLogger(MessageReader.class);

   public MessageReader(Socket socket){
       
       this.socket = socket;
   }
   
   public void run(){
       try {
           // determine the connection type to decide whether to buffer
           DataInputStream in = new DataInputStream(socket.getInputStream());
           MessagingService.validateProcotolMagic(in.readInt());
           
           int header = in.readInt();
           boolean isStream = MessagingService.getBits(header, 3, 1) == 1;
           int guessedOurVersion = MessagingService.getBits(header, 23, 8);
           
           if(isStream) {
               readStream(in, guessedOurVersion);
               return;
           }
           
           readMessage(in, header, guessedOurVersion);
                                 
       }
       catch(IOException e) {
           logger.debug("IOException reading from socket; closing", e);
       }
       finally {
           this.close();
       }
   } 
   
   private void readStream(DataInputStream in, int guessedOurVersion) throws IOException
   {
	   MessagingService.getFileStreamReader(socket, guessedOurVersion).read();
   }
   
  
   
   private void readMessage(DataInputStream in, int header, int guessedOurVersion) throws IOException{
       DataOutputStream out = new DataOutputStream(socket.getOutputStream());
       out.writeInt(MessagingService.CURRENT_VERSION);
       out.flush();
       
       int remoteVersion = MessagingService.getBits(header, 15, 8);       
       from = InetAddressSerialization.deserialize(in);
       
       MessagingService.instance().setRemoteMessagingVersion(from, Math.min(MessagingService.CURRENT_VERSION, remoteVersion));
       
       
       if(guessedOurVersion > MessagingService.CURRENT_VERSION) {
           logger.info("Received messages from newer protocol version {}. Ignoring", guessedOurVersion);
           return;
       }
       
       
       boolean compressed = MessagingService.getBits(header, 2, 1) == 1;
       if (compressed)
       {
           logger.debug("Upgrading incoming connection to be compressed");
           in = new DataInputStream(new SnappyInputStream(socket.getInputStream()));
       }
       else
       {
           in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 4096));
       } 
       while (true)
       {
           receiveMessage(in, guessedOurVersion);
       }      
   }
   
   private void receiveMessage(DataInputStream input, int version) throws IOException
   {
       String id = input.readUTF();
       String replyTo = input.readUTF();
       long timestamp = System.currentTimeMillis();
       long remoteSenderTimestamp =  input.readInt(); 
                  
       if (MessagingConfiguration.hasCrossNodeTimeout())
           timestamp = (timestamp & 0xFFFFFFFF00000000L) | (((remoteSenderTimestamp & 0xFFFFFFFFL) << 2) >> 2);
       
       MessageIn message = MessageIn.read(input, version, id, replyTo);
       if (message == null)
       {
           // callback expired; nothing to do
           return;
       }
       if (version <= MessagingService.CURRENT_VERSION)
       {
           MessagingService.instance().receive(message,  timestamp);
       }
       else
       {
           logger.debug("Received connection from newer protocol version {}. Ignoring message", version);
       }
       
       
   }
   private void close()
   {
       try
       {
           socket.close();
       }
       catch (IOException e)
       {
           if (logger.isDebugEnabled())
               logger.debug("error closing socket", e);
       }
   }
}