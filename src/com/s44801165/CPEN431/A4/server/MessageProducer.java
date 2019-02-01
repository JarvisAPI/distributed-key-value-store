package com.s44801165.CPEN431.A4.server;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;

import com.s44801165.CPEN431.A4.protocol.NetworkMessage;
import com.s44801165.CPEN431.A4.protocol.Protocol;

import ca.NetSysLab.ProtocolBuffers.KeyValueResponse;

public class MessageProducer extends Thread {
    private BlockingQueue<NetworkMessage> mBlockingQueue;
    private DatagramSocket mSocket;

    public MessageProducer(DatagramSocket socket, BlockingQueue<NetworkMessage> queue) {
        mBlockingQueue = queue;
        mSocket = socket;
    }

    @Override
    public void run() {
        byte[] maxDataBuf = NetworkMessage.getMaxDataBuffer();
        DatagramPacket packet = new DatagramPacket(maxDataBuf, maxDataBuf.length);
        DatagramPacket replyPacket = new DatagramPacket(new byte[0], 0, null, 0);
        NetworkMessage message = null;
        KeyValueResponse.KVResponse.Builder kvResBuilder = KeyValueResponse.KVResponse.newBuilder();

        while (true) {
            try {
                mSocket.receive(packet);
                message = NetworkMessage
                        .contructMessage(Arrays.copyOf(packet.getData(), packet.getLength()));
                message.setAddressAndPort(packet.getAddress(), packet.getPort());
                
                try {
                    mBlockingQueue.add(message);
                } catch (IllegalStateException e) {
                    System.out.println("Queue overload");
                    kvResBuilder.clear();
                    message.setPayload(kvResBuilder
                            .setErrCode(Protocol.ERR_SYSTEM_OVERLOAD)
                            .setOverloadWaitTime(Protocol.OVERLOAD_WAITTIME)
                            .build()
                            .toByteArray());
                    byte[] dataBytes = message.getDataBytes();
                    replyPacket.setData(dataBytes);
                    replyPacket.setAddress(packet.getAddress());
                    replyPacket.setPort(packet.getPort());
                    mSocket.send(replyPacket);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
