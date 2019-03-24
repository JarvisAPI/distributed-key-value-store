package com.g8A.CPEN431.A9.server;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.concurrent.BlockingQueue;

public class WriteEventHandler implements EventHandler {
    
    public static class WriteBundle {
        public final ByteBuffer outBuffer;
        public final SocketAddress outAddr;
        
        public WriteBundle(ByteBuffer outBuffer, SocketAddress outAddr) {
            this.outBuffer = outBuffer;
            this.outAddr = outAddr;
        }
    }
    
    public static void write(DatagramChannel channel, ByteBuffer buf, SocketAddress addr) throws IOException, InterruptedException {
        if (channel.send(buf, addr) == 0) {
            Selector sel = Reactor.getInstance().getDemultiplexer();
            SelectionKey key = channel.keyFor(sel);
            @SuppressWarnings("unchecked")
            BlockingQueue<WriteBundle> queue = (BlockingQueue<WriteBundle>) key.attachment();
            
            System.err.println("[WARNING]: WriteEventHandler#write, cannot send immediately, queuing");
            
            queue.put(new WriteBundle(buf, addr));
            
            synchronized(channel.blockingLock()) {
                key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
            }
            
            key.selector().wakeup();
        }
    }

    @Override
    public void handleEvent(SelectionKey key) {
        @SuppressWarnings("unchecked")
        BlockingQueue<WriteBundle> queue = (BlockingQueue<WriteBundle>) key.attachment();
        
        WriteBundle writeBundle = queue.poll();
        if (writeBundle != null) {
            try {
                DatagramChannel channel = (DatagramChannel) key.channel();
                System.err.println("[DEBUG]: WriteEventHandler#handleEvent, data sent");
                channel.send(writeBundle.outBuffer, writeBundle.outAddr);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        
        synchronized(key.channel().blockingLock()) {
            if (queue.isEmpty()) {
                System.err.println("[DEBUG]: WriteEventHandler#handleEvent, channel set back to read");
                key.interestOps(SelectionKey.OP_READ);
            }
        }
    }
}