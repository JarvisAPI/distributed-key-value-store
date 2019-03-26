package com.g8A.CPEN431.A11.server;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.util.concurrent.ExecutorService;

import com.g8A.CPEN431.A11.protocol.NetworkMessage;
import com.g8A.CPEN431.A11.server.distribution.EpidemicProtocol;

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
            
            int localPort = channel.socket().getLocalPort();
            if (localPort == ReactorServer.KEY_VALUE_PORT) {
                mThreadPool
                    .execute(new KeyValueRequestTask(channel, (InetSocketAddress) addr,  buffer));
            }
            else if (localPort == EpidemicProtocol.EPIDEMIC_SRC_PORT) {
                mThreadPool
                    .execute(new EpidemicProtocol.EpidemicReceiveTask(buffer));
            }
            else {
                mThreadPool
                    .execute(ReactorServer.getInstance().getPrimaryKVClient().createReceiveTask(buffer));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
