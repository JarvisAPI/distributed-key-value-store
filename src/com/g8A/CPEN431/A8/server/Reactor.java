package com.g8A.CPEN431.A8.server;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Reactor implements Runnable {
    private Selector mDemultiplexer;
    private Map<Integer, EventHandler> mHandlers;
    private static Reactor mReactor;
    
    private Reactor() throws IOException {
        mHandlers = new ConcurrentHashMap<>();
        mDemultiplexer = Selector.open();
    }
    
    public Selector getDemultiplexer() {
        return mDemultiplexer;
    }
    
    public static Reactor makeInstance() throws IOException {
        if (mReactor == null) {
            mReactor = new Reactor();
        }
        return mReactor;
    }
    
    public static Reactor getInstance() {
        return mReactor;
    }

    public void registerChannel(int eventType, SelectableChannel channel) throws ClosedChannelException {
        channel.register(mDemultiplexer, eventType);
    }
    
    public void registerEventHandler(int eventType, EventHandler handler) {
        mHandlers.put(eventType, handler);
    }

    @Override
    public void run() {
        try {
            while(true) {
                mDemultiplexer.select();
                Set<SelectionKey> readyHandles = mDemultiplexer.selectedKeys();
                Iterator<SelectionKey> handleIterator = readyHandles.iterator();
                
                while(handleIterator.hasNext()) {
                    SelectionKey handle = handleIterator.next();
                    
                    if (handle.isReadable()) {
                        mHandlers.get(SelectionKey.OP_READ).handleEvent(handle);
                        handleIterator.remove();
                    }
                }
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
        
    }
}
