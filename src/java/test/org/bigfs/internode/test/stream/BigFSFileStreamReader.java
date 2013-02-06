package org.bigfs.internode.test.stream;

import org.bigfs.internode.streaming.AFileStreamReaderTask;


public class BigFSFileStreamReader extends AFileStreamReaderTask
{

    @Override
    public void read()
    {
        System.out.println("Now you can access socket and you can read and write");        
    } 

}
