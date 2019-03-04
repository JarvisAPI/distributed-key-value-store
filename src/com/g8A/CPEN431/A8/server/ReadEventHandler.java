package com.g8A.CPEN431.A8.server;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.util.concurrent.ExecutorService;

import com.g8A.CPEN431.A8.client.PeriodicKVClient;
import com.g8A.CPEN431.A8.protocol.NetworkMessage;

public class ReadEventHandler implements EventHandler {
    private ByteBuffer mInputBuffer = ByteBuffer.wrap(NetworkMessage.getMaxDataBuffer());
    private ExecutorService mThreadPool;
    
    public ReadEventHandler(ExecutorService threadPool) {
        mThreadPool = threadPool;
    }
    
    @Override
    public void handleEvent(SelectionKey key) {
        DatagramChannel channel = (DatagramChannel) key.channel();
        try {
            mInputBuffer.clear();
            SocketAddress addr = channel.receive(mInputBuffer);
            mInputBuffer.flip();
            byte[] buffer = new byte[mInputBuffer.limit()];
            mInputBuffer.get(buffer);
            
            if (channel.socket().getLocalPort() == ReactorServer.getInstance().getKeyValuePort()) {
                mThreadPool
                    .execute(new KeyValueRequestTask(channel, (InetSocketAddress) addr,  buffer));
            }
            else {
                mThreadPool
                    .execute(new PeriodicKVClient.ReceiveTask(buffer));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
