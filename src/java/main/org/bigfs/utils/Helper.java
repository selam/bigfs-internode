package org.bigfs.utils;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class Helper
{
    public static int getAvailableProcessors()
    {
        if (System.getProperty("bigfs.available_processors") != null)
            return Integer.parseInt(System.getProperty("bigfs.available_processors"));
        else
            return Runtime.getRuntime().availableProcessors();
    }
    
    /**
     * You should almost never use this.  Instead, use the write* methods to avoid copies.
     */
    public static byte[] getArray(ByteBuffer buffer)
    {
        int length = buffer.remaining();

        if (buffer.hasArray())
        {
            int boff = buffer.arrayOffset() + buffer.position();
            if (boff == 0 && length == buffer.array().length)
                return buffer.array();
            else
                return Arrays.copyOfRange(buffer.array(), boff, boff + length);
        }
        // else, DirectByteBuffer.get() is the fastest route
        byte[] bytes = new byte[length];
        buffer.duplicate().get(bytes);

        return bytes;
    }
}
