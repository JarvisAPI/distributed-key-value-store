package com.g8A.CPEN431.A12.server;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.g8A.CPEN431.A12.client.KVClient;
import com.g8A.CPEN431.A12.protocol.NetworkMessage;
import com.g8A.CPEN431.A12.protocol.Protocol;
import com.g8A.CPEN431.A12.protocol.Util;
import com.g8A.CPEN431.A12.server.KeyValueStore.ValuePair;
import com.g8A.CPEN431.A12.server.distribution.DirectRoute;
import com.g8A.CPEN431.A12.server.distribution.HashEntity;
import com.g8A.CPEN431.A12.server.distribution.RouteStrategy;
import com.g8A.CPEN431.A12.server.distribution.RouteStrategy.AddressHolder;
import com.google.protobuf.ByteString;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueRequest.KVRequest;

public class MigrateKVHandler implements KVClient.OnResponseReceivedListener {
	private int NUM_OF_PUTS = 100;
	private int RETRY_INTERVAL = 100;
	private int BATCH_INTERVAL = 10000; // Amount of time to wait to batch migrate.
	private Set<Integer> mJoiningNodeIdx;
	private volatile boolean mTimerStarted;
	private static MigrateKVHandler mHandler;
	private KVClient mKVClient;
	private RouteStrategy mRouteStrat;
	

    private MigrateKVHandler(KVClient kvClient) {
        mJoiningNodeIdx = new HashSet<>();
        mKVClient = kvClient;
        mRouteStrat = DirectRoute.getInstance();
        mKVClient.setResponseListener(this);
    }
    
    /**
     * 
     * @param nodeId the node id from hashing
     */
    public void migrate(int nodeId) {
        NetworkMessage isAliveMessage = new NetworkMessage(Util.getUniqueId());
        AddressHolder addr = mRouteStrat.getRoute(nodeId);
        isAliveMessage.setPayload(KVRequest.newBuilder().setCommand(Protocol.IS_ALIVE).build().toByteArray());
        isAliveMessage.setAddressAndPort(addr.address, addr.port);
        
        mKVClient.send(isAliveMessage, null, nodeId);
    }
    
    @Override
    public void onResponseReceived(int requestId, NetworkMessage msg) {
        synchronized(mHandler) {
            if (!mTimerStarted) {
                mTimerStarted = true;
                Util.scheduler.schedule(new MigrateKVTask(), BATCH_INTERVAL, TimeUnit.MILLISECONDS);
            }
            mJoiningNodeIdx.add(requestId);
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
    
    public static MigrateKVHandler makeInstance(KVClient kvClient) {
        if (mHandler == null) {
            mHandler = new MigrateKVHandler(kvClient);
        }
        return mHandler;
    }
    
    public static MigrateKVHandler getInstance() {
        return mHandler;
    }

    public class MigrateKVTask implements Runnable {
        @Override
        public void run() {
            Set<Integer> nodeIdSet = new HashSet<>();
            synchronized(mHandler) {
                nodeIdSet.addAll(mJoiningNodeIdx);
                mTimerStarted = false;
            }
            try {
                KeyValueStore kvStore = KeyValueStore.getInstance();
            	HashEntity hashEntity = HashEntity.getInstance(); 	
            	Set<ByteString> keySet = KeyValueStore.getInstance().getKeys();
            	ValuePair vPair;
            	int tries = 0;
        
            	byte[] dataBuf;
                NetworkMessage message;
                KeyValueRequest.KVRequest.Builder kvReqBuilder = KeyValueRequest.KVRequest.newBuilder();
        
                int nodeId;

                System.out.println("[INFO]: Starting migration task");
                System.out.println(String.format("[INFO]: Migrated nodeIds: %s", Arrays.asList(nodeIdSet).toString()));
                System.out.println(String.format("[INFO]: Migration checking %d keys", keySet.size()));
                int numKeysMigrated = 0;
                
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
                    if (nodeIdSet.contains(nodeId)) {
            			// send put request to new node
                        numKeysMigrated++;
                        
            			vPair = kvStore.get(key);
            			AddressHolder toAddress = mRouteStrat.getRoute(nodeId);
            			
            			dataBuf = kvReqBuilder.setCommand(Protocol.PUT)
            					.setKey(key)        					
                                .setValue(vPair.value)
                                .setVersion(vPair.version)
                                .setSequenceStamp(vPair.sequenceStamp)
                                .build()
                                .toByteArray();
       
	                    message = new NetworkMessage(Util.getUniqueId(toAddress.port));
	                    message.setPayload(dataBuf);
	                    message.setAddressAndPort(toAddress.address, toAddress.port);
	                    mKVClient.send(message, null);
	                    kvStore.remove(key);
                	}
                }
            	
            	System.out.println(String.format("[INFO]: Migrated %d keys", numKeysMigrated));
            } catch (Exception e2) {
                e2.printStackTrace();
            } finally {
                synchronized(mHandler) {
                    mJoiningNodeIdx.removeAll(nodeIdSet);
                }   
            }
        }
    }

    
}

