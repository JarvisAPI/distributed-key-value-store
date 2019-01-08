package com.s44801165.CPEN431.A1.protocol;

import java.util.Arrays;

public class NetworkMessage {
    public static final int ID_SIZE = 16; // in bytes
    private static final int MAX_PAYLOAD_SIZE = 16 * 1024; // in bytes
    private byte[] mUniqueId;
    private byte[] mPayload;

    public NetworkMessage(byte[] uniqueId) {
        mUniqueId = uniqueId;
    }
    
    public void setPayload(byte[] payload) {
        int payloadSize = payload.length;
        if (payloadSize > MAX_PAYLOAD_SIZE) {
            System.out.println("Payload in request message is too long, truncating...");
            payloadSize = MAX_PAYLOAD_SIZE;
        }
        mPayload = new byte[payloadSize];
        System.arraycopy(payload, 0, mPayload, 0, payloadSize);
    }
    
    /**
     * Get bytes to send.
     * @return all bytes that describes the message.
     */
    public byte[] getDataBytes() {
        byte[] dataBytes = new byte[ID_SIZE + mPayload.length];
        System.arraycopy(mUniqueId, 0, dataBytes, 0, ID_SIZE);
        System.arraycopy(mPayload, 0, dataBytes, ID_SIZE, mPayload.length);
        return dataBytes;
    }
    
    /**
     * 
     * @return the maximum data byte buffer that network messages need.
     */
    public static byte[] getMaxDataBuffer() {
        return new byte[ID_SIZE  + MAX_PAYLOAD_SIZE];
    }
    
    public static NetworkMessage contructReplyMessage(byte[] data) {
        NetworkMessage msg = new NetworkMessage(Arrays.copyOf(data, ID_SIZE));
        int payloadSize = data.length - ID_SIZE;
        if (payloadSize > MAX_PAYLOAD_SIZE) {
            System.out.println("Payload in reply message is too long, truncating...");
            payloadSize = MAX_PAYLOAD_SIZE;
        }
        msg.mPayload = new byte[payloadSize];
        System.arraycopy(data, ID_SIZE, msg.mPayload, 0, payloadSize);
        return msg;
    }
    
    public byte[] getId() {
        return mUniqueId;
    }
    
    public byte[] getPayload() {
        return mPayload;
    }
}
