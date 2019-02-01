package com.s44801165.CPEN431.A4.test;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.HashMap;
import java.util.Map;

import com.google.protobuf.ByteString;
import com.s44801165.CPEN431.A4.client.BaseClient;
import com.s44801165.CPEN431.A4.protocol.NetworkMessage;
import com.s44801165.CPEN431.A4.protocol.Protocol;
import com.s44801165.CPEN431.A4.protocol.Util;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse;

public class KVTestClient extends BaseClient {
    private InetAddress serverAddr;
    private int serverPort;
    private Inet4Address myAddr;
    private Map<ByteString, MessageBundle> bundleMap;
    
    private int receivedCount = 0;
    private int sendCount = 0;
    
    private int state;
    
    private static final int TEST_AT_MOST_ONCE_SEMANTICS = 1;
    private static final int TEST_GET_PID = 2;
    
    public static class MessageBundle {
        public ByteString key;
        public ByteString value;
        public NetworkMessage reply;
        public int version;
        public boolean received;
    }
    
    public KVTestClient() {
        bundleMap = new HashMap<>();
    }

    public void setArgs(String[] args) throws Exception {
        if (args.length < 2) {
            throw new IllegalArgumentException("Not enough args");
        }
        serverAddr = InetAddress.getByName(args[0]);
        serverPort = Integer.parseInt(args[1]);
    }

    @Override
    public void runClient() {
        try {
            super.runClient();
            myAddr = (Inet4Address) InetAddress.getLocalHost();

            testAtMostOnceSemantics();
            testGetPid();
        } catch (SocketException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            quit();
        }
    }
    
    private void resetSendCount() {
        sendCount = 0;
        receivedCount = 0;
    }
    
    /**
     * Test the server adheres to at-most-one semantics.
     * Two PUT request with same message ID, get sucess for first PUT then PUT
     * again with different value, should return cached result.
     * 
     * Request order:
     *  PUT -> PUT -> GET
     *  
     *  @throws Exception
     */
    private void testAtMostOnceSemantics() throws Exception {
        bundleMap.clear();
        state = TEST_AT_MOST_ONCE_SEMANTICS;
        resetSendCount();
        
        final int SIZE_VALUE = 4;
        ByteString firstValue;
        ByteString key = generateRandomKey();
        MessageBundle bundle = new MessageBundle();
        NetworkMessage msg = generateTestMessage(Protocol.PUT,
                SIZE_VALUE,
                key,
                bundle);
        firstValue = bundle.value;
        
        bundleMap.put(msg.getIdString(), bundle);
        sendPacket(msg);
        sendCount++;
        
        waitForMessages();
        
        resetSendCount();
        byte[] value = new byte[SIZE_VALUE];
        for (int i = 0; i < value.length; i++) value[i] = 0x00;
        
        bundle.value = ByteString.copyFrom(value);
        bundle.received = false;
        
        KeyValueRequest.KVRequest.Builder kvBuilder = KeyValueRequest.KVRequest
                .newBuilder()
                .setKey(key)
                .setCommand(Protocol.PUT)
                .setValue(bundle.value)
                .setVersion(1);
        
        msg.setPayload(kvBuilder.build().toByteArray());
        bundleMap.put(msg.getIdString(), bundle);
        sendPacket(msg);
        sendCount++;
        
        waitForMessages();
        
        resetSendCount();
        bundleMap.remove(msg.getIdString());
        msg = generateTestMessage(Protocol.GET,
                0,
                key,
                null);
        bundle.received = false;
        bundleMap.put(msg.getIdString(), bundle);
        sendPacket(msg);
        sendCount++;
        
        waitForMessages();
        
        NetworkMessage reply = bundle.reply;
        KeyValueResponse.KVResponse kvRes = KeyValueResponse.KVResponse
                .parseFrom(reply.getPayload());
        
        if (kvRes.getValue().equals(firstValue)) {
            System.out.println("Server adheres to at-most-once semantics");
            passed();
        }
        else {
            System.out.println("Server does not adhere to at-most-once semantics");
            failed();
        }
    }
    
    /**
     * Tests the get pid command, should return success.
     * 
     * @throws Exception
     */
    private void testGetPid() throws Exception {
        bundleMap.clear();
        state = TEST_GET_PID;
        resetSendCount();
        
        MessageBundle bundle = new MessageBundle();
        NetworkMessage msg = generateTestMessage(Protocol.GET_PID,
                0,
                ByteString.EMPTY,
                null);
        bundleMap.put(msg.getIdString(), bundle);
        sendPacket(msg);
        sendCount++;
        
        waitForMessages();
        NetworkMessage reply = bundle.reply;
        
        KeyValueResponse.KVResponse kvRes = KeyValueResponse.KVResponse
                .parseFrom(reply.getPayload());
        
        if (kvRes.getErrCode() == Protocol.ERR_SUCCESS) {
            System.out.println("PID: " + kvRes.getPid());
            passed();
        }
        else {
            System.out.println("Get PID returned non-success error code: " + kvRes.getErrCode());
            failed();
        }
    }
    
    private void passed() {
        System.out.println("TEST PASSED");
    }
    
    private void failed() {
        System.out.println("TEST FAILED");
    }

    @Override
    protected void onMessageReceived(NetworkMessage message) {
        if (state == TEST_AT_MOST_ONCE_SEMANTICS || state == TEST_GET_PID) {
            MessageBundle bundle = bundleMap.get(message.getIdString());
            if (bundle != null && !bundle.received) {
                bundle.received = true;
                bundle.reply = message;
                receivedCount++;
            }
            if (sendCount == receivedCount) {
                stopWaiting();
                return;
            }
        }
    }

    private ByteString generateRandomKey() {
        byte[] randKey = new byte[Protocol.SIZE_MAX_KEY_LENGTH];
        for (int i = 0; i < randKey.length; i++) {
            randKey[i] = (byte) (Math.random() * 256);
        }
        return ByteString.copyFrom(randKey);
    }

    /**
     * Generate message with given value size. Every byte in the payload generated
     * is 0xFF and the version is 0, if the command is PUT.
     * 
     * @param command
     *            the command to send.
     * @param valueSize
     *            the size of the value field if command is PUT.
     * @param key
     *            the value of the key to use in the request.
     * @param msgBundle
     *            the generated message parameters is placed into this object
     *            if the request is a put,
     * @return the network message to be sent over the network.
     */
    private NetworkMessage generateTestMessage(int command,
            int valueSize,
            ByteString key,
            MessageBundle msgBundle) {
        NetworkMessage msg = new NetworkMessage(Util.getUniqueId(myAddr, serverPort));
        
        KeyValueRequest.KVRequest.Builder kvBuilder = KeyValueRequest.KVRequest
                .newBuilder()
                .setKey(key)
                .setCommand(command);

        if (command == Protocol.PUT) {
            byte[] value = new byte[valueSize];
            for (int i = 0; i < valueSize; i++) {
                value[i] = (byte) 0xff;
            }
            ByteString bvalue = ByteString.copyFrom(value);
            kvBuilder.setValue(bvalue);
            kvBuilder.setVersion(0);
            
            if (msgBundle != null) {
                msgBundle.key = key;
                msgBundle.value = bvalue;
                msgBundle.version = 0;
                msgBundle.received = false;
            }
        }
        
        byte[] payload = kvBuilder
                .build()
                .toByteArray();

        msg.setAddressAndPort(serverAddr, serverPort);
        msg.setPayload(payload);
        return msg;
    }

    public static void main(String[] args) throws Exception {
        KVTestClient client = new KVTestClient();
        client.setArgs(args);
        client.runClient();
    }
}
