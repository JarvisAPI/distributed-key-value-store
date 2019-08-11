package com.dkvstore;

import java.net.Inet4Address;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.dkvstore.server.ReactorServer;
import com.dkvstore.server.distribution.DirectRoute;
import com.google.protobuf.ByteString;

public class Util {
    public static final Random rand = new Random();
    public static final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    
    public static byte[] getUniqueId() {
        return getUniqueId((Inet4Address) DirectRoute.getInstance().getLocalAddress().address, ReactorServer.KEY_VALUE_PORT);
    }
    
    public static byte[] getUniqueId(int port) {
        return getUniqueId((Inet4Address) DirectRoute.getInstance().getLocalAddress().address, port);
    }
    
    /**
     * Create a network message id used for transmission.
     * @param addr the ip4 address of the client making the request.
     * @param port the port of the server that client is making request to.
     */
    public static byte[] getUniqueId(Inet4Address addr, int port) {
        byte[] uniqueId = new byte[NetworkMessage.ID_SIZE];
        int idx = 0;
        int ip4Len = 4;
        System.arraycopy(addr.getAddress(), 0, uniqueId, 0, ip4Len);
        
        idx += ip4Len;
        uniqueId[idx++] = (byte) (port >>> 8);
        uniqueId[idx++] = (byte) port;
        
        uniqueId[idx++] = (byte) (Math.random() * 255);
        uniqueId[idx++] = (byte) (Math.random() * 255);
        
        long nanoTime = System.nanoTime();
        uniqueId[idx++] = (byte) (nanoTime >>> 56);
        uniqueId[idx++] = (byte) (nanoTime >>> 48);
        uniqueId[idx++] = (byte) (nanoTime >>> 40);
        uniqueId[idx++] = (byte) (nanoTime >>> 32);
        
        uniqueId[idx++] = (byte) (nanoTime >>> 24);
        uniqueId[idx++] = (byte) (nanoTime >>> 16);
        uniqueId[idx++] = (byte) (nanoTime >>> 8);
        uniqueId[idx++] = (byte) (nanoTime);
        
        return uniqueId;
    }
    
    public static void printHexString(byte[] dataBytes) {
        for (byte b : dataBytes) {
            System.out.print(String.format("%02X", b));
        }
        System.out.println();
    }
    
    public static String getHexString(byte[] dataBytes) {
        StringBuilder builder = new StringBuilder();
        for (byte b : dataBytes) {
            builder.append(hexToChar((b >> 4) & 0x0f));
            builder.append(hexToChar(b & 0x0f));
        }
        return builder.toString();
    }
    
    public static char hexToChar(int b) {
        if (b >= 10 && b < 16) {
            return (char) ('A' + b - 10);
        }
        else if (b >= 0 && b < 10) {
            return (char) ('0' + b);
        }
        return '0';
    }
    
    public static ByteString concatHostnameAndPort(String addr, int port) {
    	return ByteString.copyFrom(addr.concat(String.valueOf(port)).getBytes());
    }
    
    /**
     * The bytes are in big endian format
     * @param num
     * @param buf
     * @param start
     */
    public static void longToBytes(long num, byte[] buf, int start) {
        buf[start++] = (byte) (num >>> 56);
        buf[start++] = (byte) (num >>> 48);
        buf[start++] = (byte) (num >>> 40);
        buf[start++] = (byte) (num >>> 32);
        buf[start++] = (byte) (num >>> 24); 
        buf[start++] = (byte) (num >>> 16);
        buf[start++] = (byte) (num >>> 8);
        buf[start++] = (byte) num;
    }
    
    /**
     * The bytes are in big endian format
     * @param num
     * @param buf
     * @param start
     */
    public static void intToBytes(int num, byte[] buf, int start) {
        buf[start++] = (byte) (num >>> 24); 
        buf[start++] = (byte) (num >>> 16);
        buf[start++] = (byte) (num >>> 8);
        buf[start++] = (byte) num;
    }
    
    /**
     * Get int from bytes in big endian format.
     * @param data
     * @param start
     * @return
     */
    public static int intFromBytes(byte[] data, int start) {
        int num = data[start++] & 0xff;
        num = num << 8;
        num |= (data[start++] & 0xff);
        num = num << 8;
        num |= (data[start++] & 0xff);
        num = num << 8;
        num |= (data[start++] & 0xff);
        return num;
    }
    
    /**
     * Get long from bytes in big endian format.
     * @param data
     * @param start
     * @return
     */
    public static long longFromBytes(byte[] data, int start) {
        int mostSignificant = intFromBytes(data, start);
        int leastSignificant = intFromBytes(data, start + 4);
        
        long num = mostSignificant;
        num = num << 32;
        num |= leastSignificant;
        return num;
    }
}
