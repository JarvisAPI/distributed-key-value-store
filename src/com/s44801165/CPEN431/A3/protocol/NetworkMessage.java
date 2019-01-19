package com.s44801165.CPEN431.A3.protocol;

import java.io.IOException;
import java.net.InetAddress;
import java.util.zip.CRC32;
import com.google.protobuf.ByteString;

import ca.NetSysLab.ProtocolBuffers.Message;

public class NetworkMessage {
    public static final int ID_SIZE = 16; // in bytes
    private static final int MAX_PAYLOAD_SIZE = 16 * 1024; // in bytes

    private ByteString mUniqueId;
    private ByteString mPayload;
    private InetAddress mAddress;
    private int mPort;

    public NetworkMessage() {
    }

    public NetworkMessage(byte[] uniqueId) {
        mUniqueId = ByteString.copyFrom(uniqueId);
    }

    public void setPayload(byte[] payload) {
        int payloadSize = payload.length;
        if (payloadSize > MAX_PAYLOAD_SIZE) {
            System.out.println("Payload in request message is too long, truncating...");
            payloadSize = MAX_PAYLOAD_SIZE;
        }
        mPayload = ByteString.copyFrom(payload, 0, payloadSize);
    }

    /**
     * Get bytes to send.
     * 
     * @return all bytes that describes the message.
     */
    public byte[] getDataBytes() {
        CRC32 crc = new CRC32();
        crc.update(mUniqueId.toByteArray());
        crc.update(mPayload.toByteArray());
        Message.Msg msg = Message.Msg.newBuilder()
                .setMessageID(mUniqueId)
                .setPayload(mPayload)
                .setCheckSum(crc.getValue())
                .build();
        return msg.toByteArray();
    }

    /**
     * 
     * @return the maximum data byte buffer that network messages need.
     */
    public static byte[] getMaxDataBuffer() {
        return new byte[ID_SIZE + MAX_PAYLOAD_SIZE];
    }

    public static NetworkMessage contructMessage(byte[] data) throws IOException {
        Message.Msg transportMsg = Message.Msg.newBuilder().mergeFrom(data).build();
        byte[] id = transportMsg.getMessageID().toByteArray();
        byte[] payload = transportMsg.getPayload().toByteArray();
        long checksum = transportMsg.getCheckSum();
        if (!validateChecksum(id, payload, checksum)) {
            throw new IOException("Checksum doesn't match");
        }
        NetworkMessage msg = new NetworkMessage(id);
        msg.mPayload = ByteString.copyFrom(payload);
        return msg;
    }

    public static void setMessage(NetworkMessage msg, byte[] data) throws IOException {
        Message.Msg transportMsg = Message.Msg.newBuilder().mergeFrom(data).build();
        ByteString id = transportMsg.getMessageID();
        ByteString payload = transportMsg.getPayload();
        long checksum = transportMsg.getCheckSum();
        if (!validateChecksum(id.toByteArray(), payload.toByteArray(), checksum)) {
            throw new IOException("Checksum doesn't match");
        }
        msg.mUniqueId = id;
        msg.mPayload = payload;
    }

    private static boolean validateChecksum(byte[] id, byte[] payload, long checksum) {
        CRC32 crc = new CRC32();
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

    public byte[] getPayload() {
        return mPayload.toByteArray();
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
