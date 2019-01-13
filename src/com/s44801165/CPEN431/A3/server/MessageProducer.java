package com.s44801165.CPEN431.A3.server;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;

import com.s44801165.CPEN431.A3.protocol.NetworkMessage;
import com.s44801165.CPEN431.A3.protocol.Protocol;

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
        NetworkMessage message = null;

        while (true) {
            try {
                mSocket.receive(packet);
                message = NetworkMessage
                        .contructMessage(Arrays.copyOf(packet.getData(), packet.getLength()));
                message.setAddressAndPort(packet.getAddress(), packet.getPort());
    
                try {
                    mBlockingQueue.add(message);
                } catch (IllegalStateException e) {
                    message.setPayload(KeyValueResponse.KVResponse.newBuilder()
                            .setErrCode(Protocol.ERR_SYSTEM_OVERLOAD)
                            .build()
                            .toByteArray());
                    byte[] dataBytes = message.getDataBytes();
                    mSocket.send(new DatagramPacket(dataBytes, dataBytes.length,
                            packet.getAddress(), packet.getPort()));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
