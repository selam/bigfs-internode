package org.bigfs.internode.test;

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

import org.bigfs.concurrent.ThreadPoolExecutorFactory;
import org.bigfs.internode.configuration.MessagingConfiguration;
import org.bigfs.internode.message.IAsyncResult;
import org.bigfs.internode.message.MessageOut;
import org.bigfs.internode.service.MessagingService;
import org.bigfs.internode.test.stream.BigFSFileStreamHeader;

public class Deneme
{
    
   
    
    public static void main(String args[]) throws Exception{
        MessagingConfiguration.setListenAddress(InetAddress.getLocalHost());
        MessagingService.instance().listen();
        
        MessagingService.registerMessageSerializer(Test.messageType, Test.serializer);
        MessagingService.registerMessageSerializer(BigFSFileStreamHeader.messageType, BigFSFileStreamHeader.serializer);
        MessagingService.registerMessageSerializer(TestRequestResponse.messageType, TestRequestResponse.serializer);
        MessagingService.registerMessageGroupExecutor(Test.messageGroup, ThreadPoolExecutorFactory.multiThreadedExecutor("Test", "TestExecutor", 10));
        MessagingService.registerMessageHandlers(Test.messageType, new TestMessageHandler());
        MessagingService.registerMessageHandlers(TestRequestResponse.messageType, new TestRequestResponseMessageHandler());
        
        MessagingService.registerFileStreamReaderClass("org.bigfs.internode.test.BigFSFileStreamReader");

        MessagingService.registerCompressedFileStreamMessageClass("org.bigfs.internode.test.CompressedFileStreamMessage");
        MessagingService.registerFileStreamMessageClass("org.bigfs.internode.test.FileStreamMessage");
        
        Test test = new Test("Deneme mesajı one way");
        
        MessageOut<Test> m = new MessageOut<Test>(test.getMessageGroup(), test, Test.serializer);
        // send and forget
        MessagingService.instance().sendOneWay(m, InetAddress.getLocalHost());
        
        
        TestRequestResponse t = new TestRequestResponse("Deneme mesajı needed to response");
        MessageOut<TestRequestResponse> m2 = new MessageOut<TestRequestResponse>(t.getMessageGroup(), t, TestRequestResponse.serializer);

        // send message and wait response
        IAsyncResult result = MessagingService.instance().send(m2, InetAddress.getLocalHost());
        Test r = (Test) result.get(MessagingConfiguration.getRpcTimeout(), TimeUnit.MILLISECONDS);
        System.out.println(r.getText());
        
        // Send file
        BigFSFileStreamHeader header = new BigFSFileStreamHeader();        
        MessagingService.instance().send(header, InetAddress.getLocalHost());
        
        
    }
}
