package com.dkvstore;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.dkvstore.NetworkMessage;
import com.dkvstore.Protocol;
import com.dkvstore.Util;
import com.dkvstore.A7TestClient.Entry;
import com.google.protobuf.ByteString;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse;

public class Stage3Test {
    private DatagramSocket mSocket;
    private DatagramPacket mReceivePacket;
    private int mDefaultServerPort;
    private InetAddress mServerAddr;
    private int randomKeyLength = 8;
    private int numNodes = 11;
    private int numKeys = 1;
    private int[] nodePorts;
    private List<Integer> testValues;
    private Set<ByteString> testKeys;
    
    private Stage3Test(int serverPort) {
        mServerAddr = InetAddress.getLoopbackAddress();
        mDefaultServerPort = serverPort;
        byte[] buf = NetworkMessage.getMaxDataBuffer();
        mReceivePacket = new DatagramPacket(buf, buf.length);
        nodePorts = new int[numNodes];
        for(int i = 0; i < numNodes; i++) {
        	nodePorts[i] = serverPort + i;
        }
        testKeys = new HashSet<ByteString>();
        testValues = new ArrayList<Integer>();
    }
    
    
    private NetworkMessage generateTestPut(int valueSize, ByteString key, Entry entry, int serverPort, ByteString value, int version) {
        NetworkMessage msg = new NetworkMessage(Util.getUniqueId((Inet4Address) InetAddress.getLoopbackAddress(), serverPort));
        
        KeyValueRequest.KVRequest.Builder kvBuilder = KeyValueRequest.KVRequest
                .newBuilder()
                .setKey(key)
                .setCommand(Protocol.PUT);

//        byte[] value = new byte[valueSize];
//        for (int i = 0; i < valueSize; i++) {
//            value[i] = (byte) 0xff;
//        }
//        ByteString bvalue = ByteString.copyFrom(value);
        kvBuilder.setValue(value);
        kvBuilder.setVersion(0);
        
        if (entry != null) {
            entry.value = value;
            entry.version = 0;
        }
        
        byte[] payload = kvBuilder
                .build()
                .toByteArray();

        msg.setAddressAndPort(mServerAddr, serverPort);
        msg.setPayload(payload);
        return msg;
    }
    
    private NetworkMessage generateTestGet(ByteString key, int serverPort) {
        NetworkMessage msg = new NetworkMessage(Util.getUniqueId((Inet4Address) InetAddress.getLoopbackAddress(), serverPort));
        
        KeyValueRequest.KVRequest.Builder kvBuilder = KeyValueRequest.KVRequest
                .newBuilder()
                .setKey(key)
                .setCommand(Protocol.GET);
        
        byte[] payload = kvBuilder
                .build()
                .toByteArray();
        
        msg.setAddressAndPort(mServerAddr, serverPort);
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
    
    private ByteString generateKey(int key) {
        byte[] randKey = new byte[randomKeyLength];
        Util.intToBytes(key, randKey, 0);
        return ByteString.copyFrom(randKey);

    }
    
    private void run() throws Exception {
        mSocket = new DatagramSocket(60111);
        DatagramPacket packet = new DatagramPacket(new byte[0], 0);
        packet.setAddress(mServerAddr);
        packet.setPort(mDefaultServerPort);
        
        // set up keys and values
        testKeys.add(generateKey(23));
        testKeys.add(generateKey(57));
        
        testValues.add(891);
        
        // perform tests
        for(int i = 0; i < testValues.size(); i++) {
        	System.out.print("\n");
        	for(ByteString key : testKeys) {

                Entry entry = new Entry();
                
                byte[] val = new byte[randomKeyLength];
                Util.intToBytes(testValues.get(i), val, 0); // key value is 13
                entry.value = ByteString.copyFrom(val);
                entry.version = i;
                
                NetworkMessage msg = generateTestPut(32, key, entry, mDefaultServerPort, entry.value, entry.version);
                packet.setAddress(msg.getAddress());
                packet.setPort(msg.getPort());
                packet.setData(msg.getDataBytes());
                
                System.out.println("Sent put request to " + msg.getPort() + ": key=" + Util.intFromBytes(key.toByteArray(), 0) + " val=" + Util.intFromBytes(entry.value.toByteArray(),0) + " size: " + packet.getLength());
                mSocket.send(packet);
                
                mSocket.receive(mReceivePacket);
                byte[] data = Arrays.copyOf(mReceivePacket.getData(), mReceivePacket.getLength());
                NetworkMessage.setMessage(msg, data);
                
                KeyValueResponse.KVResponse kvRes = KeyValueResponse.KVResponse.parseFrom(msg.getPayload());
                System.out.println("Put response err Code: " + kvRes.getErrCode());

            }
            
        	for(int j = 0; j < 10; j++) {
        		System.out.println("wait 1 sec");
                TimeUnit.SECONDS.sleep(1);
                System.out.println("Sending gets round "+ j);
                for(ByteString key : testKeys) {
                	NetworkMessage msg = generateTestGet(key, mDefaultServerPort);
                	packet.setAddress(msg.getAddress());
                    packet.setPort(msg.getPort());
                    packet.setData(msg.getDataBytes());
                    
                    System.out.println("Sent get request, size: " + packet.getLength());
                    System.out.println("Sent get request to " + msg.getPort() + ": key=" + Util.intFromBytes(key.toByteArray(), 0) + " size: " + packet.getLength());
                    mSocket.send(packet);
                    
                    mSocket.receive(mReceivePacket);
                    byte[] data = Arrays.copyOf(mReceivePacket.getData(), mReceivePacket.getLength());
                    NetworkMessage.setMessage(msg, data);
                    
                    KeyValueResponse.KVResponse kvRes = KeyValueResponse.KVResponse.parseFrom(msg.getPayload());
                    System.out.print("Get response err Code: " + kvRes.getErrCode());
                    if(kvRes.getValue().isEmpty()) {
                    	System.out.print(" val=EMPTY\n");
                    }else {
                    	System.out.print(" val=" + Util.intFromBytes(kvRes.getValue().toByteArray(), 0) + " ver=" + kvRes.getVersion() + "\n");
                    }
                }
        	}
            
        }
        System.out.println("TEST COMPLETE");
    }
    
    public static void main(String args[]) throws Exception {
        Stage3Test client = new Stage3Test(50137);
        client.run();
    }
}
