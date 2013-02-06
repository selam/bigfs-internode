package org.bigfs.internode.serialization;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;

public class InetAddressSerialization
{
    public static void serialize(InetAddress endpoint, DataOutput dos) throws IOException
    {
        byte[] buf = endpoint.getAddress();
        dos.writeByte(buf.length);
        dos.write(buf);
    }

    public static InetAddress deserialize(DataInput dis) throws IOException
    {
        byte[] bytes = new byte[dis.readByte()];
        dis.readFully(bytes, 0, bytes.length);
        return InetAddress.getByAddress(bytes);
    }

    public static int serializedSize(InetAddress from)
    {
        if (from instanceof Inet4Address)
            return 1 + 4;
        assert from instanceof Inet6Address;
        return 1 + 16;
    }
}
