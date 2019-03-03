package com.g8A.CPEN431.A8.server;

import java.net.Inet4Address;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.g8A.CPEN431.A8.client.KVClient;
import com.g8A.CPEN431.A8.protocol.NetworkMessage;
import com.g8A.CPEN431.A8.protocol.Protocol;
import com.g8A.CPEN431.A8.protocol.Util;
import com.g8A.CPEN431.A8.server.KeyValueStore.ValuePair;
import com.g8A.CPEN431.A8.server.distribution.DirectRoute;
import com.g8A.CPEN431.A8.server.distribution.HashEntity;
import com.g8A.CPEN431.A8.server.distribution.NodeTable;
import com.g8A.CPEN431.A8.server.distribution.RouteStrategy.AddressHolder;
import com.google.protobuf.ByteString;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;

public class MigrateKVThread implements Runnable {
	private AddressHolder mToAddress;
	private int NUM_OF_PUTS = 5;
	private int RETRY_INTERVAL = 20;
	private BlockingQueue<Integer> mJoiningNodeIdx;
	private static MigrateKVThread mMigrateThread;
	private KVClient mClient;

    private MigrateKVThread(KVClient client) {
        mJoiningNodeIdx = new LinkedBlockingQueue<>();
        mClient = client;
    }
    
    /**
     * 
     * @param nodeId the node id from hashing
     */
    public synchronized void migrate(int nodeId) {
        if (!mJoiningNodeIdx.contains(nodeId)) {
            mJoiningNodeIdx.add(nodeId);
        }
    }
    
    /**
     * 
     * @param nodeId the node id from hashing
     * @return true if in the process of migrating given nodeIdx.
     */
    public boolean isMigrating(int nodeId) {
        return mJoiningNodeIdx.contains(nodeId);
    }
    
    public boolean isMigrating() {
        return !mJoiningNodeIdx.isEmpty();
    }
    
    public static MigrateKVThread makeInstance(KVClient client) {
        if (mMigrateThread == null) {
            mMigrateThread = new MigrateKVThread(client);
        }
        return mMigrateThread;
    }
    
    public static MigrateKVThread getInstance() {
        return mMigrateThread;
    }

    public void run() {
        while (true) {
            try {
                mJoiningNodeIdx.take();
                
            	HashEntity hashEntity = HashEntity.getInstance(); 	
            	Set<ByteString> keySet = KeyValueStore.getInstance().getKeys();
            	ValuePair vPair;
            	int tries = 0;
        
            	byte[] dataBuf;
                NetworkMessage message;
                KeyValueRequest.KVRequest.Builder kvReqBuilder = KeyValueRequest.KVRequest.newBuilder();
        
            	
                boolean keySent;
                int nodeId;
                int selfNodeId = DirectRoute.getInstance().getSelfNodeId();
                
                Inet4Address selfAddress;
                try {
                    selfAddress = (Inet4Address) NodeTable.getInstance().getSelfAddressHolder().address;
                } catch (Exception e) {
                    selfAddress = (Inet4Address) Inet4Address.getLoopbackAddress();
                }
            	for(ByteString key : keySet) {
            	    nodeId = hashEntity.getKVNodeId(key);
                	
                	if (tries == NUM_OF_PUTS) {
                		tries = 0;
                        try {
                            Thread.sleep(RETRY_INTERVAL);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                	} else {
                		tries++;
                	}
                    if (nodeId != selfNodeId) {
            			// send put request to new node
            			vPair = KeyValueStore.getInstance().get(key);
            			
            			dataBuf = kvReqBuilder.setCommand(Protocol.PUT)
            					.setKey(key)        					
                                .setValue(vPair.value)
                                .setVersion(vPair.version)
                                .build()
                                .toByteArray();
            			
            	        keySent = false;
            	        while (!keySent) {
                			try {
        	                    message = new NetworkMessage(Util.getUniqueId(selfAddress, mToAddress.port));
        	                    message.setPayload(dataBuf);
        	                    message.setAddressAndPort(mToAddress.address, mToAddress.port);
        	                    mClient.send(message, null);
        	                    keySent = true;
        	                    KeyValueStore.getInstance().remove(key);
                			} catch (IllegalStateException e) {
                                try {
                                    Thread.sleep(RETRY_INTERVAL);
                                } catch (InterruptedException e1) {
                                    // Ignored
                                }
                			}
            	        }
                	}
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
    }

    
}

