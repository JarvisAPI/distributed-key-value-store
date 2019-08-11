package com.dkvstore;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.dkvstore.NetworkMessage;
import com.dkvstore.Protocol;
import com.dkvstore.Util;
import com.dkvstore.server.distribution.RouteStrategy.AddressHolder;
import com.google.protobuf.ByteString;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse;
import ca.NetSysLab.ProtocolBuffers.KeyValueRequest.KVRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse.KVResponse;

/**
 * 1. Suspends n nodes
 * 2. Give 90 seconds to recover
 * 3. Add 20,000 keys, check that they are indeed stored
 * 4. Resume k < n nodes, k > 0
 * 5. Attempt to get keys back
 *
 */
public class Stage2cTest {
    private DatagramSocket mSocket;
    private DatagramPacket mReceivePacket;
    private int randomKeyLength = 8;
    private Random rnd = new Random(0);
    private int numNodesToSuspend = 10;
    private int numNodesToResume = 5;
    
    public class Entry {
        ByteString key;
        ByteString value;
        int version;
    }
    
    private Map<ByteString, Entry> mKeyMap = new HashMap<>();
    private List<AddressHolder> servers = new ArrayList<>();
    private List<AddressHolder> suspendedServers = new ArrayList<>();
    private Map<AddressHolder, Integer> pidMap = new HashMap<>();
    
    private Stage2cTest() {
        byte[] buf = NetworkMessage.getMaxDataBuffer();
        mReceivePacket = new DatagramPacket(buf, buf.length);
    }
    
    
    private NetworkMessage generateTestPut(int valueSize, ByteString key, Entry entry) {
        NetworkMessage msg = new NetworkMessage(Util.getUniqueId((Inet4Address) InetAddress.getLoopbackAddress(), rnd.nextInt()));
        
        KeyValueRequest.KVRequest.Builder kvBuilder = KeyValueRequest.KVRequest
                .newBuilder()
                .setKey(key)
                .setCommand(Protocol.PUT);

        byte[] value = new byte[valueSize];
        for (int i = 0; i < valueSize; i++) {
            value[i] = (byte) rnd.nextInt(256);
        }
        ByteString bvalue = ByteString.copyFrom(value);
        kvBuilder.setValue(bvalue);
        kvBuilder.setVersion(0);
        
        if (entry != null) {
            entry.key = key;
            entry.value = bvalue;
            entry.version = 0;
        }
        
        byte[] payload = kvBuilder
                .build()
                .toByteArray();

        msg.setPayload(payload);
        return msg;
    }
    
    private NetworkMessage generateTestGet(ByteString key) {
        NetworkMessage msg = new NetworkMessage(Util.getUniqueId((Inet4Address) InetAddress.getLoopbackAddress(), 50111));
        
        KeyValueRequest.KVRequest.Builder kvBuilder = KeyValueRequest.KVRequest
                .newBuilder()
                .setKey(key)
                .setCommand(Protocol.GET);
        
        byte[] payload = kvBuilder
                .build()
                .toByteArray();
        
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
    
    private void getPids() throws IOException {
        System.out.println("Getting server pids");
        
        for (AddressHolder server : servers) {
            KeyValueRequest.KVRequest.Builder kvReq = KVRequest.newBuilder();
            kvReq.setCommand(Protocol.GET_PID);
            NetworkMessage msg = new NetworkMessage(Util.getUniqueId((Inet4Address)InetAddress.getLoopbackAddress(), rnd.nextInt()));
            msg.setPayload(kvReq.build().toByteArray());
            msg.setAddressAndPort(server.address, server.port);
            
            send(msg);
            KVResponse kvRes = receive(msg.getIdString());
            
            if (kvRes.getErrCode() == Protocol.ERR_SUCCESS) {
                pidMap.put(server, kvRes.getPid());
            }
            else {
                System.err.println("[ERROR]: Cannot get pid abort...");
                System.exit(1);
            }
        }
    }
    
    private void suspendNodes() throws IOException, InterruptedException {
        List<AddressHolder> randServers = new ArrayList<>(servers);
        Collections.shuffle(randServers, rnd);
        
        for (int i = 0; i < numNodesToSuspend; i++) {
            suspendedServers.add(randServers.get(i));
            
            int pid = pidMap.get(randServers.get(i));
            
            System.out.println("[INFO]: Suspending: pid: " + pid + ", " + randServers.get(i).hostname + ":" + randServers.get(i).port);
            Process proc = new ProcessBuilder("kill", "-stop", Integer.toString(pid)).start();
            proc.waitFor();
            //printErrStream(proc);
        }
    }
    
    private void printErrStream(Process proc) throws IOException {
        BufferedReader reader = 
                new BufferedReader(new InputStreamReader(proc.getErrorStream()));
        StringBuilder builder = new StringBuilder();
        String line = null;
        while ( (line = reader.readLine()) != null) {
           builder.append(line);
           builder.append(System.getProperty("line.separator"));
        }
        String result = builder.toString();
        System.err.println(result);
    }
    
    private void run() throws Exception {
        mSocket = new DatagramSocket();
        mSocket.setSoTimeout(2000);
        
        getPids();
        
        System.out.println("Wipeout All");
        for (AddressHolder node : servers) {
            KVRequest.Builder b = KVRequest.newBuilder();
            b.setCommand(Protocol.WIPEOUT);
            NetworkMessage msg = new NetworkMessage(Util.getUniqueId((Inet4Address)InetAddress.getLoopbackAddress(), rnd.nextInt()));
            msg.setPayload(b.build().toByteArray());
            msg.setAddressAndPort(node.address, node.port);
            
            send(msg);
            receive(msg.getIdString());
        }
        
        // Step 1.
        System.out.println(String.format("[INFO]: Suspending %d nodes", numNodesToSuspend));
        suspendNodes();
        System.out.println("[INFO]: Waiting 90s");
        Thread.sleep(90000);
        
        // Step 2.
        servers.removeAll(suspendedServers);
        int numPuts = 0;
        System.out.println("[INFO]: Putting keys");
        List<Entry> ls = new ArrayList<>();
        int numKeysToAdd = 20000;
        for (int i = 0; i < numKeysToAdd; i++) {
            ByteString key = generateRandomKey();
            Entry entry = new Entry();
            NetworkMessage msg = generateTestPut(500, key, entry);
            AddressHolder node = servers.get(rnd.nextInt(servers.size()));
            msg.setAddressAndPort(node.address, node.port);
            
            send(msg);
            KVResponse kvRes = receive(msg.getIdString());
            if (kvRes == null) {
                continue;
            }
            
            if (kvRes.getErrCode() == Protocol.ERR_SUCCESS) {
                numPuts++;
            }
            else {
                System.out.println("PUT ErrCode: " + kvRes.getErrCode());
            }
            ls.add(entry);
            if (i % 1000 == 0) {
                Thread.sleep(600);
            }
        }
        System.out.println("[INFO]: Number of successful puts: " + numPuts);
        
        
        // Step 3.
        int i = 0;
        System.out.println("[INFO]: Getting keys after PUT");
        for (Entry e : ls) {
            NetworkMessage msg = generateTestGet(e.key);
            AddressHolder node = servers.get(rnd.nextInt(servers.size()));
            msg.setAddressAndPort(node.address, node.port);
            
            send(msg);
            
            KVResponse kvRes = receive(msg.getIdString());
            if (kvRes == null) {
                continue;
            }
            
            if (kvRes.getErrCode() == Protocol.ERR_SUCCESS) {
                if (!kvRes.getValue().equals(e.value)) {
                    System.out.println("Error after " + i + " GETs");
                    System.out.println(Util.getHexString(kvRes.getValue().toByteArray()));
                    System.out.println(Util.getHexString(e.value.toByteArray()));
                    System.out.println("[ERROR]: GET != PUT");
                    System.exit(1);
                }
                mKeyMap.put(e.key, e);
            }
            else {
                System.out.println("Get ErrCode: " + kvRes.getErrCode());
            }
            i++;
            if (i % 1000 == 0) {
                Thread.sleep(600);
            }
        }
        System.out.println("[INFO]: Num successful GETS: " + mKeyMap.size());
        
        // Step 4.
        System.out.println(String.format("[INFO]: Resuming %d nodes", numNodesToResume));
        resumeNodes();
        System.out.println("[INFO]: Waiting 90s");
        Thread.sleep(90000);
        
        // Step 5.
        System.out.println("[INFO]: Getting keys after nodes resumed");
        int numSuccess = 0;
        int numKeyNotFound = 0;
        for (Entry e : mKeyMap.values()) {
            NetworkMessage msg = generateTestGet(e.key);
            AddressHolder node = servers.get(rnd.nextInt(servers.size()));
            msg.setAddressAndPort(node.address, node.port);
            
            send(msg);
            
            KVResponse kvRes = receive(msg.getIdString());
            if (kvRes == null) {
                continue;
            }
            
            if (kvRes.getErrCode() == Protocol.ERR_SUCCESS) {
                if (!kvRes.getValue().equals(e.value)) {
                    System.out.println("[ERROR]: GET != PUT");
                    System.exit(1);
                }
                numSuccess++;
            }
            else if (kvRes.getErrCode() == Protocol.ERR_NON_EXISTENT_KEY) {
                numKeyNotFound++;
            }
        }
        System.out.println("[INFO]: Number of successful GET: " + numSuccess);
        System.out.println("[INFO]: Number of key not found: " + numKeyNotFound);
        
        System.out.println("[INFO]: Shutting down servers");
        servers.addAll(suspendedServers);
        for (AddressHolder node : servers) {
            KVRequest.Builder b = KVRequest.newBuilder();
            b.setCommand(Protocol.SHUTDOWN);
            NetworkMessage msg = new NetworkMessage(Util.getUniqueId((Inet4Address)InetAddress.getLoopbackAddress(), rnd.nextInt()));
            msg.setPayload(b.build().toByteArray());
            msg.setAddressAndPort(node.address, node.port);
            send(msg);
        }
        /*
        System.out.println("[INFO]: Resuming remaining servers");
        for (AddressHolder node : suspendedServers) {
            Process proc = new ProcessBuilder("kill", "-cont", Integer.toString(pidMap.get(node))).start();
            proc.waitFor();
        }*/
    }
    
    private void resumeNodes() throws IOException, InterruptedException { 
        List<AddressHolder> resumeList = new ArrayList<>(suspendedServers);
        Collections.shuffle(resumeList, rnd);
        
        for (int i = 0; i < numNodesToResume; i++) {
            AddressHolder node = resumeList.get(i);
            Process proc = new ProcessBuilder("kill", "-cont", Integer.toString(pidMap.get(node))).start();
            proc.waitFor();
            System.out.println("[INFO]: Resumed: " + node.hostname + ":" + node.port);
            suspendedServers.remove(node);
            servers.add(node);
        }
    }
    
    private KVResponse receive(ByteString msgIdString) throws IOException {
        //System.out.println("Message Id String: " + Util.getHexString(msgIdString.toByteArray()));
        NetworkMessage msg;
        KeyValueResponse.KVResponse kvRes = null;
        try {
            do {
                mSocket.receive(mReceivePacket);
                msg = NetworkMessage.contructMessage(Arrays.copyOf(mReceivePacket.getData(),
                        mReceivePacket.getLength()));
                
                kvRes = KVResponse.parseFrom(msg.getPayload());
            } while(!msg.getIdString().equals(msgIdString));
        } catch(SocketTimeoutException e) {
            kvRes = null;
            System.out.println("[INFO]: Request timed out");
        }
        
        return kvRes;
    }
    
    private void send(NetworkMessage msg) throws IOException {
        byte[] buf = msg.getDataBytes();
        DatagramPacket packet = new DatagramPacket(buf, buf.length, msg.getAddress(), msg.getPort());
        
        mSocket.send(packet);
    }
    
    private void readServerFile(String serverFile) throws IOException {
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(serverFile));
            String line = reader.readLine();
            while (line != null) {
                String[] e = line.split(":");
                servers.add(new AddressHolder(InetAddress.getByName(e[0]), e[0], Integer.parseInt(e[1])));
                line = reader.readLine();
            }
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
    }
    
    public static void main(String args[]) throws Exception {
        Stage2cTest client = new Stage2cTest();
        client.readServerFile(args[0]);
        client.run();
    }
}
