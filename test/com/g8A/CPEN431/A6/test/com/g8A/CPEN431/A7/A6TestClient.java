package com.g8A.CPEN431.A7;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.Arrays;

import com.g8A.CPEN431.A7.protocol.NetworkMessage;
import com.g8A.CPEN431.A7.protocol.Protocol;
import com.g8A.CPEN431.A7.protocol.Util;
import com.g8A.CPEN431.A7.server.distribution.RouteStrategy.AddressHolder;
import com.google.protobuf.ByteString;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse.KVResponse;

public class A6TestClient {
    private InetAddress serverAddr;
    private int serverPort;
    private DatagramSocket mSocket;
    private DatagramPacket mReceivePacket;
    private AddressHolder[] mAddressAndPorts;
    
    public A6TestClient() throws SocketException {
        mSocket = new DatagramSocket();  
        byte[] dataBytes = NetworkMessage.getMaxDataBuffer();
        mReceivePacket = new DatagramPacket(dataBytes, dataBytes.length);
    }
    
    public void runClient() throws Exception {
        System.out.println("Client running");
        for (AddressHolder addressHolder : mAddressAndPorts) {
            shutdown(addressHolder.address, addressHolder.port);
        }
        /*
        for (int i = 0; i < 1225; i++) {
            closedLoop(Protocol.SIZE_MAX_VAL_LENGTH);
        }
        Thread.sleep(10 * 1000);
        System.out.println("First closed loop ended, waiting 10 seconds");
        for (int i = 0; i < 2; i++) {
            closedLoop(Protocol.SIZE_MAX_VAL_LENGTH/2);
        }*/
    }
    
    private void closedLoop(int valueSize) throws Exception {
        int errCode;
        ByteString key = generateRandomKey();
        NetworkMessage msg = generateTestPut(valueSize, key);
        sendPacket(msg);
        mSocket.receive(mReceivePacket);
        NetworkMessage.setMessage(msg, Arrays.copyOf(mReceivePacket.getData(),
                                                     mReceivePacket.getLength()));
        
        KVResponse builder = KVResponse.parseFrom(msg.getPayload());
        errCode = builder.getErrCode();
        System.out.println("PUT errCode: " + errCode);
        
        msg = generateTestGet(key);
        sendPacket(msg);
        mSocket.receive(mReceivePacket);
        NetworkMessage.setMessage(msg, Arrays.copyOf(mReceivePacket.getData(),
                mReceivePacket.getLength()));
        
        builder = KVResponse.parseFrom(msg.getPayload());
        errCode = builder.getErrCode();
        System.out.println("GET errCode: " + errCode);
        Thread.sleep(10);
    }
    
    private void shutdown(InetAddress address, int port) throws Exception {
        KeyValueRequest.KVRequest.Builder kvBuilder = KeyValueRequest.KVRequest
                .newBuilder()
                .setCommand(Protocol.SHUTDOWN);
        NetworkMessage msg = new NetworkMessage(Util.getUniqueId((Inet4Address) InetAddress.getLoopbackAddress(), serverPort));
        msg.setAddressAndPort(address, port);
        msg.setPayload(kvBuilder.build().toByteArray());
        sendPacket(msg);
    }
    
    protected final void sendPacket(NetworkMessage message) throws IOException {
        byte[] dataBytes = message.getDataBytes();
        DatagramPacket packet = new DatagramPacket(dataBytes, dataBytes.length,
                message.getAddress(), message.getPort());
        mSocket.send(packet);
    }
    
    private void setAddressAndPort(InetAddress addr, int port) {
        serverAddr = addr;
        serverPort = port;
    }
    
    private void setAddressAndPorts(AddressHolder[] addressAndPorts) {
        mAddressAndPorts = addressAndPorts;
    }
    
    private ByteString generateRandomKey() {
        byte[] randKey = new byte[Protocol.SIZE_MAX_KEY_LENGTH];
        for (int i = 0; i < randKey.length; i++) {
            randKey[i] = (byte) (Math.random() * 256);
        }
        return ByteString.copyFrom(randKey);
    }
    
    private NetworkMessage generateTestPut(int valueSize, ByteString key) {
        NetworkMessage msg = new NetworkMessage(Util.getUniqueId((Inet4Address) InetAddress.getLoopbackAddress(), serverPort));
        
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
        
        byte[] payload = kvBuilder
                .build()
                .toByteArray();

        msg.setAddressAndPort(serverAddr, serverPort);
        msg.setPayload(payload);
        return msg;
    }
    
    private NetworkMessage generateTestGet(ByteString key) {
        NetworkMessage msg = new NetworkMessage(Util.getUniqueId((Inet4Address) InetAddress.getLoopbackAddress(), serverPort));
        
        KeyValueRequest.KVRequest.Builder kvBuilder = KeyValueRequest.KVRequest
                .newBuilder()
                .setKey(key)
                .setCommand(Protocol.GET);
        
        byte[] payload = kvBuilder
                .build()
                .toByteArray();
        
        msg.setAddressAndPort(serverAddr, serverPort);
        msg.setPayload(payload);
        return msg;
    }
    
    public static void main(String[] args) throws Exception {
        A6TestClient client = new A6TestClient();
        InetAddress addr = InetAddress.getLoopbackAddress();
        int port = 8082;
        //client.setAddressAndPort(addr, port);
        String[] hostAndPort = {
                "planetlab1.cs.ubc.ca:50111",
                "planetlab2.cs.ubc.ca:50111",
                "pl1.rcc.uottawa.ca:50111"
                };
        AddressHolder[] addrAndPorts = new AddressHolder[hostAndPort.length];
        int i = 0;
        for (String entry : hostAndPort) {
            String[] hp = entry.split(":");
            addrAndPorts[i++] = new AddressHolder(InetAddress.getByName(hp[0]), Integer.parseInt(hp[1]));
        }
        client.setAddressAndPorts(addrAndPorts);
        client.runClient();
    }
}