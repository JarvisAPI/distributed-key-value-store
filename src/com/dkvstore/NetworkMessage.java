package com.dkvstore;

import java.io.IOException;
import java.net.InetAddress;
import java.util.zip.CRC32;
import com.google.protobuf.ByteString;

import ca.NetSysLab.ProtocolBuffers.Message;

public class NetworkMessage {
    public static final int ID_SIZE = 16; // in bytes
    public static final int MAX_PAYLOAD_SIZE = 16 * 1024; // in bytes

    private ByteString mUniqueId;
    private byte[] mUniqueIdBytes;
    private ByteString mPayload;
    private byte[] mPayloadBytes;
    private InetAddress mAddress;
    private int mPort;
    private CRC32 mCrc = new CRC32();

    public NetworkMessage() {
    }

    public NetworkMessage(byte[] uniqueId) {
        mUniqueIdBytes = uniqueId;
        mUniqueId = ByteString.copyFrom(uniqueId);
    }

    public void setPayload(byte[] payload) {
        if (payload.length > MAX_PAYLOAD_SIZE) {
            System.out.println("[WARNING]: Payload in request message is too long, truncating...");
            
            mPayload = ByteString.copyFrom(payload, 0, MAX_PAYLOAD_SIZE);
            mPayloadBytes = mPayload.toByteArray();
        }
        else {
            mPayload = ByteString.copyFrom(payload);
            mPayloadBytes = payload;
        }
    }

    /**
     * Get bytes to send.
     * 
     * @return all bytes that describes the message.
     */
    public byte[] getDataBytes() {
        CRC32 crc = new CRC32();
        
        crc.update(mUniqueIdBytes);
        crc.update(mPayloadBytes);
        return Message.Msg.newBuilder()
                .setMessageID(mUniqueId)
                .setPayload(mPayload)
                .setCheckSum(crc.getValue())
                .build()
                .toByteArray();
    }

    /**
     * 
     * @return the maximum data byte buffer that network messages need.
     */
    public static byte[] getMaxDataBuffer() {
        return new byte[ID_SIZE + MAX_PAYLOAD_SIZE];
    }

    public static NetworkMessage contructMessage(byte[] data) throws IOException {
        Message.Msg transportMsg = Message.Msg.parseFrom(data);
        ByteString payloadString = transportMsg.getPayload();
        ByteString idString = transportMsg.getMessageID();
        byte[] id = idString.toByteArray();
        byte[] payload = payloadString.toByteArray();
        long checksum = transportMsg.getCheckSum();
        if (!validateChecksum(new CRC32(), id, payload, checksum)) {
            throw new IOException("Checksum doesn't match");
        }
        NetworkMessage msg = new NetworkMessage();
        msg.mUniqueId = idString;
        msg.mUniqueIdBytes = id;
        msg.mPayload = payloadString;
        msg.mPayloadBytes = payload;
        return msg;
    }
    
    /**
     * Clones a given NetworkMessage to have the same content as the original but with a different unique Id.
     * @param original the NetworkMessage to clone.
     * @param uniqueId the unique id bytes of the clone.
     * @return a newly cloned NetworkMessage.
     */
    public static NetworkMessage clone(NetworkMessage original, byte[] uniqueId) {
        NetworkMessage clonedMessage = new NetworkMessage(uniqueId);
        clonedMessage.setPayload(original.getPayload());
        return clonedMessage;
    }

    public static void setMessage(NetworkMessage msg, byte[] data) throws IOException {
        Message.Msg transportMsg = Message.Msg.parseFrom(data);
        ByteString id = transportMsg.getMessageID();
        ByteString payload = transportMsg.getPayload();
        long checksum = transportMsg.getCheckSum();
        byte[] idBytes = id.toByteArray();
        byte[] payloadBytes = payload.toByteArray();
        if (!validateChecksum(msg.mCrc, idBytes, payloadBytes, checksum)) {
            throw new IOException("Checksum doesn't match");
        }
        msg.mUniqueId = id;
        msg.mUniqueIdBytes = idBytes;
        msg.mPayload = payload;
        msg.mPayloadBytes = payloadBytes;
    }

    private static boolean validateChecksum(CRC32 crc, byte[] id, byte[] payload, long checksum) {
        crc.reset();
        crc.update(id);
        crc.update(payload);

        return crc.getValue() == checksum;
    }

    public byte[] getId() {
        return mUniqueId.toByteArray();
    }

    public ByteString getIdString() {
        return mUniqueId;
    }
    
    public void setIdString(ByteString id) {
        mUniqueId = id;
        mUniqueIdBytes = id.toByteArray();
    }

    public byte[] getPayload() {
        return mPayloadBytes;
    }

    public ByteString getPayloadString() {
        return mPayload;
    }

    public void setAddressAndPort(InetAddress addr, int port) {
        mAddress = addr;
        mPort = port;
    }

    public InetAddress getAddress() {
        return mAddress;
    }

    public int getPort() {
        return mPort;
    }

}
