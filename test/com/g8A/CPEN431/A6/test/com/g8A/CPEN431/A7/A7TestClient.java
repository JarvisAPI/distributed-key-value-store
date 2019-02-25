package com.g8A.CPEN431.A7;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.g8A.CPEN431.A7.protocol.NetworkMessage;
import com.g8A.CPEN431.A7.protocol.Protocol;
import com.g8A.CPEN431.A7.protocol.Util;
import com.g8A.CPEN431.A7.server.distribution.RouteStrategy.AddressHolder;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse.KVResponse;

public class A7TestClient {
    private InetAddress serverAddr;
    private int serverPort;
    private DatagramSocket mSocket;
    private DatagramPacket mReceivePacket;
    private AddressHolder[] mAddressAndPorts;
    private int randomKeyLength = Protocol.SIZE_MAX_KEY_LENGTH;
    
    public static class Entry {
        public ByteString value;
        public int version;
    }
    private Map<ByteString, Entry> mMap;
    
    public A7TestClient() throws SocketException {
        mSocket = new DatagramSocket();  
        byte[] dataBytes = NetworkMessage.getMaxDataBuffer();
        mReceivePacket = new DatagramPacket(dataBytes, dataBytes.length);
        mMap = new HashMap<>();
    }
    
    public void runClient() throws Exception {
        System.out.println("Client running");
        System.out.println("[INFO]: Phase 1");
        randomKeyLength = 4;
        int numKeys = 100;
        serverAddr = mAddressAndPorts[0].address;
        serverPort = mAddressAndPorts[0].port;
        for (int i = 0; i < numKeys; i++) {
            doPUT(Protocol.SIZE_MAX_VAL_LENGTH);
        }
        System.out.println("Expecting 2nd node to join");
        Thread.sleep(30 * 1000);
        System.out.println("[INFO]: Phase 2");
        
        Entry receivedEntry, entry;
        int count = 0;
        for (ByteString key : mMap.keySet()) {
            receivedEntry = doGET(key);
            entry = mMap.get(key);
            if (receivedEntry != null) {
                if (!entry.value.equals(receivedEntry.value) ||
                    entry.version != receivedEntry.version) {
                    System.err.println("[ERROR]: Wrong GET value or version");
                }
            }
            else {
                count++;
            }
        }
        System.out.println(String.format("[INFO]: Number of non-successful GETs: %d", count));
    }
    
    public void doPUT(int valueSize) throws Exception {
        int errCode;
        ByteString key = generateRandomKey();
        Entry entry = new Entry();
        NetworkMessage msg = generateTestPut(valueSize, key, entry);
        sendPacket(msg);
        // Assumes server setup is local.
        mSocket.receive(mReceivePacket);
        NetworkMessage.setMessage(msg, Arrays.copyOf(mReceivePacket.getData(),
                                                     mReceivePacket.getLength()));
        
        KVResponse builder = KVResponse.parseFrom(msg.getPayload());
        errCode = builder.getErrCode();
        if (errCode != 0) {
            System.out.println("Non-zero PUT errCode: " + errCode);
        }
        
        mMap.put(key, entry);
    }
    
    public Entry doGET(ByteString key) throws Exception {
        int errCode;
        Entry entry = new Entry();
        NetworkMessage msg = generateTestGet(key);
        sendPacket(msg);
        mSocket.receive(mReceivePacket);
        NetworkMessage.setMessage(msg, Arrays.copyOf(mReceivePacket.getData(),
                mReceivePacket.getLength()));
        
        KVResponse builder = KVResponse.parseFrom(msg.getPayload());
        errCode = builder.getErrCode();
        if (errCode != 0) {
            System.out.println("Non-zero GET errCode: " + errCode);
            return null;
        }
        entry.value = builder.getValue();
        entry.version = builder.getVersion();
        return entry;
    }
    
    private void closedLoop(int valueSize) throws Exception {
        int errCode;
        ByteString key = generateRandomKey();
        NetworkMessage msg = generateTestPut(valueSize, key, null);
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
    
    private void setAddressAndPorts(AddressHolder[] addressAndPorts) {
        mAddressAndPorts = addressAndPorts;
    }
    
    private ByteString generateRandomKey() {
        byte[] randKey = new byte[randomKeyLength];
        for (int i = 0; i < randKey.length; i++) {
            randKey[i] = (byte) (Math.random() * 256);
        }
        return ByteString.copyFrom(randKey);
    }
    
    private NetworkMessage generateTestPut(int valueSize, ByteString key, Entry entry) {
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
        
        if (entry != null) {
            entry.value = bvalue;
            entry.version = 0;
        }
        
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
        A7TestClient client = new A7TestClient();
        if (args.length == 2) {
            if (args[0].equals("--shutdown-all")) {
                BufferedReader reader = null;
                try {
                    reader = new BufferedReader(new FileReader(args[1]));
                    String line = reader.readLine();
                    int counter;
                    int tryCount = 3;
                    while (line != null) {
                        counter = 0;
                        String[] hostAndPort = line.split(":");
                        System.out.println("Shutting down: " + line);
                        while (counter < tryCount) {
                            client.shutdown(InetAddress.getByName(hostAndPort[0]), Integer.parseInt(hostAndPort[1]));
                            Thread.sleep(50);
                            counter++;
                        }
                        line = reader.readLine();
                    }
                } finally {
                    if (reader != null) {
                        reader.close();
                    }
                }
            }
            return;
        }
        String[] hostAndPort = {
                "127.0.0.1:50111",
                "127.0.0.1:50112"
                };
        AddressHolder[] addrAndPorts = new AddressHolder[hostAndPort.length];
        int i = 0;
        for (String entry : hostAndPort) {
            String[] hp = entry.split(":");
            addrAndPorts[i++] = new AddressHolder(InetAddress.getByName(hp[0]), hp[0], Integer.parseInt(hp[1]));
        }
        client.setAddressAndPorts(addrAndPorts);
        client.runClient();
    }
}