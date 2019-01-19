package com.s44801165.CPEN431.A3.test;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;

import com.s44801165.CPEN431.A3.client.BaseClient;
import com.s44801165.CPEN431.A3.protocol.NetworkMessage;
import com.s44801165.CPEN431.A3.protocol.Protocol;
import com.s44801165.CPEN431.A3.protocol.Util;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;

public class KVTestClient extends BaseClient {
    private InetAddress serverAddr;
    private int serverPort;
    
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
            testStressMemoryUtilization();
        } catch (SocketException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    /**
     * Stress test the server by doing PUT enough times to place approximately
     * 44MB of data in the key value store, then get all of the 44MB of values
     * back should succeed and then remove all the key-values.
     */
    private void testStressMemoryUtilization() {
        InetAddress addr;
        try {
            addr = InetAddress.getLocalHost();
            NetworkMessage msg = new NetworkMessage(Util.getUniqueId((Inet4Address) addr, serverPort));
            KeyValueRequest.KVRequest kvReq = KeyValueRequest.KVRequest.newBuilder()
                    .setCommand(Protocol.IS_ALIVE)
                    .build();
            msg.setAddressAndPort(serverAddr, serverPort);
            msg.setPayload(kvReq.toByteArray());
            sendPacket(msg);
            
            waitForMessages();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
    
    public static void main(String[] args) throws Exception {
        KVTestClient client = new KVTestClient();
        client.setArgs(args);
        client.runClient();
    }

    @Override
    protected void onMessageReceived(NetworkMessage message) {
        System.out.println("Received message with id: ");
        Util.printHexString(message.getId());
        System.out.println("Message content: ");
        Util.printHexString(message.getDataBytes());
        quit();
    }
}
