package com.g8A.CPEN431.A6.protocol;

import java.net.Inet4Address;

import com.google.protobuf.ByteString;

public class Util {
    
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
            builder.append(hexToChar((b >> 4)));
            builder.append(b & 0x0f);
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
}
