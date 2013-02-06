package org.bigfs.internode.configuration;

import java.net.InetAddress;

public class MessagingConfiguration
{
    private static int port = 8080;
    
    private static InetAddress listenAddress;
    
    private static int rpc_timeout = 1000;
    private static boolean is_compression_enable = false;
    private static int streaming_socket_timeout_in_ms = 0;
    /**
     * @return the port
     */
    public static int getPort()
    {
        return port;
    }
    /**
     * @param port the port to set
     */
    public static void setPort(int port)
    {
        MessagingConfiguration.port = port;
    }
    
    /**
     * @return the listenAddress
     */
    public static InetAddress getListenAddress()
    {        
        return listenAddress;
    }
    /**
     * @param listenAddress the listenAddress to set
     */
    public static void setListenAddress(InetAddress listenAddress)
    {
        MessagingConfiguration.listenAddress = listenAddress;
    }
    public static int getRpcTimeout()
    {
        return rpc_timeout;
    }
    public static void setRpcTimeout(int rpc_timeout)
    {
        MessagingConfiguration.rpc_timeout = rpc_timeout;
    }
    
    public static boolean isCompressionEnable()
    {
        return MessagingConfiguration.is_compression_enable;
    }
    
    public static void setCompressionEnabled(boolean is_compression_enable)
    {
        MessagingConfiguration.is_compression_enable = is_compression_enable;
    }
    
    public static boolean hasCrossNodeTimeout()
    {
        return true;
    }
    
    public static int getStreamingSocketTimeout()
    {
        return MessagingConfiguration.streaming_socket_timeout_in_ms;
    }
    
    public static void setStreamingSocketTimeout(int timeout)
    {
        MessagingConfiguration.streaming_socket_timeout_in_ms = timeout;
    }
}
