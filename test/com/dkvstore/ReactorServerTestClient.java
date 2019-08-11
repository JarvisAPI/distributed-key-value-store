package com.dkvstore;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.util.Arrays;

import com.dkvstore.NetworkMessage;
import com.dkvstore.Protocol;
import com.dkvstore.Util;
import com.dkvstore.A7TestClient.Entry;
import com.google.protobuf.ByteString;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse;

public class ReactorServerTestClient {
    private DatagramSocket mSocket;
    private DatagramPacket mReceivePacket;
    private int mServerPort;
    private InetAddress mServerAddr;
    private int randomKeyLength = 8;
    
    private ReactorServerTestClient(int serverPort) {
        mServerAddr = InetAddress.getLoopbackAddress();
        mServerPort = serverPort;
        byte[] buf = NetworkMessage.getMaxDataBuffer();
        mReceivePacket = new DatagramPacket(buf, buf.length);
    }
    
    
    private NetworkMessage generateTestPut(int valueSize, ByteString key, Entry entry) {
        NetworkMessage msg = new NetworkMessage(Util.getUniqueId((Inet4Address) InetAddress.getLoopbackAddress(), mServerPort));
        
        KeyValueRequest.KVRequest.Builder kvBuilder = KeyValueRequest.KVRequest
                .newBuilder()
                .setKey(key)
                .setCommand(Protocol.PUT);

        byte[] value = new byte[valueSize];
        for (int i = 0; i < valueSize; i++) {
            value[i] = (byte) 0xff;
        }
        ByteString bvalue = ByteString.copyFrom(value);
        kvBuilder.setValue(bvalue);
        kvBuilder.setVersion(0);
        
        if (entry != null) {
            entry.value = bvalue;
            entry.version = 0;
        }
        
        byte[] payload = kvBuilder
                .build()
                .toByteArray();

        msg.setAddressAndPort(mServerAddr, mServerPort);
        msg.setPayload(payload);
        return msg;
    }
    
    private NetworkMessage generateTestGet(ByteString key) {
        NetworkMessage msg = new NetworkMessage(Util.getUniqueId((Inet4Address) InetAddress.getLoopbackAddress(), mServerPort));
        
        KeyValueRequest.KVRequest.Builder kvBuilder = KeyValueRequest.KVRequest
                .newBuilder()
                .setKey(key)
                .setCommand(Protocol.GET);
        
        byte[] payload = kvBuilder
                .build()
                .toByteArray();
        
        msg.setAddressAndPort(mServerAddr, mServerPort);
        msg.setPayload(payload);
        return msg;
    }
    
    private ByteString generateRandomKey() {
        byte[] randKey = new byte[randomKeyLength];
        for (int i = 0; i < randKey.length; i++) {
            randKey[i] = (byte) (Math.random() * 256);
        }
        return ByteString.copyFrom(randKey);
    }
    
    private void run() throws Exception {
        mSocket = new DatagramSocket(60111);
        DatagramPacket packet = new DatagramPacket(new byte[0], 0);
        packet.setAddress(mServerAddr);
        packet.setPort(mServerPort);
        
        int i = 0;
        while (i < 10) {
            ByteString key = generateRandomKey();
            Entry entry = new Entry();
            NetworkMessage msg = generateTestPut(32, key, entry);
            packet.setAddress(msg.getAddress());
            packet.setPort(msg.getPort());
            packet.setData(msg.getDataBytes());
            
            System.out.println("Sent request, size: " + packet.getLength());
            mSocket.send(packet);
            
            mSocket.receive(mReceivePacket);
            byte[] data = Arrays.copyOf(mReceivePacket.getData(), mReceivePacket.getLength());
            NetworkMessage.setMessage(msg, data);
            
            KeyValueResponse.KVResponse kvRes = KeyValueResponse.KVResponse.parseFrom(msg.getPayload());
            System.out.println("Err Code: " + kvRes.getErrCode());
            i++;
        }
    }
    
    public static void main(String args[]) throws Exception {
        ReactorServerTestClient client = new ReactorServerTestClient(50111);
        client.run();
    }
}
