package com.dkvstore.server;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.ArrayList;
import java.util.List;

import com.dkvstore.NetworkMessage;
import com.dkvstore.Protocol;
import com.dkvstore.Util;
import com.dkvstore.client.KVClient;
import com.dkvstore.server.KeyValueStore.ValuePair;
import com.dkvstore.server.MessageCache.CacheEntry;
import com.dkvstore.server.distribution.DirectRoute;
import com.dkvstore.server.distribution.EpidemicProtocol;
import com.dkvstore.server.distribution.HashEntity;
import com.dkvstore.server.distribution.RouteStrategy;
import com.dkvstore.server.distribution.VectorClock;
import com.dkvstore.server.distribution.VirtualNode;
import com.dkvstore.server.distribution.RouteStrategy.AddressHolder;
import com.google.protobuf.ByteString;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse.KVResponse;

public class KeyValueRequestTask implements Runnable {
    private static final int CACHE_META_COMPLETE_RESPONSE = 0;
    private static final int CACHE_META_SUCCESS_BYTES = 1;
    private static final int CACHE_META_SUCCESS_GET = 2;
    private byte[] mDataBytes;
    private static KeyValueStore mKeyValStore;
    private static MessageCache mMessageCache;
    private static HashEntity mHashEntity;
    private static RouteStrategy mRouteStrat;
    private static MigrateKVHandler mMigrateKVHandler;
    private static int mNodeId;
    private static KVClient mKVClient;
    private static KVClient mSecondaryKVClient;
    private DatagramChannel mChannel;
    private KeyValueRequest.KVRequest.Builder kvReqBuilder;
    private InetSocketAddress mAddr;
    private static final byte[] SUCCESS_BYTES = KeyValueResponse.KVResponse.newBuilder()
            .setErrCode(Protocol.ERR_SUCCESS)
            .build()
            .toByteArray();

    public KeyValueRequestTask(DatagramChannel channel, InetSocketAddress addr, byte[] dataBytes) {
        mChannel = channel;
        mDataBytes = dataBytes;
        mAddr = addr;
    }
    
    public static void init() {
        mKeyValStore = KeyValueStore.getInstance();
        mMessageCache = MessageCache.getInstance();
        mHashEntity = HashEntity.getInstance();
        mRouteStrat = DirectRoute.getInstance();
        mNodeId = DirectRoute.getInstance().getSelfNodeId();
        mKVClient = ReactorServer.getInstance().getPrimaryKVClient();
        mSecondaryKVClient = ReactorServer.getInstance().getSecondaryKVClient();
        mMigrateKVHandler = MigrateKVHandler.getInstance();
    }

    @Override
    public void run() {
        try {   
            kvReqBuilder = KeyValueRequest.KVRequest.newBuilder();
            KeyValueResponse.KVResponse.Builder kvResBuilder = KeyValueResponse.KVResponse.newBuilder();
    
            NetworkMessage message = NetworkMessage.contructMessage(mDataBytes);
            message.setAddressAndPort(mAddr.getAddress(), mAddr.getPort());
            
            KeyValueStore.ValuePair vPair;
            byte[] dataBytes = null;
            int errCode;
            ByteString key;
            ByteString value;
            int cacheMetaInfo;
            int metaInfo;
    
            errCode = Protocol.ERR_SUCCESS;           
            cacheMetaInfo = 0;
            
            try {
                CacheEntry entry = mMessageCache.get(message.getIdString());
                if (entry != null) {
                    ByteString cachedMessageVal = entry.value;
                    if (cachedMessageVal == MessageCache.ENTRY_BEING_PROCESSED) {
                        // Message is being processed by other thread.
                        return;
                    }
                    metaInfo = (entry.metaInfo & 0x0000ffff);
                    switch(metaInfo) {
                    case CACHE_META_SUCCESS_GET: {
                        dataBytes = kvResBuilder
                                .setErrCode(Protocol.ERR_SUCCESS)
                                .setValue(cachedMessageVal)
                                .setVersion(entry.intField0)
                                .build()
                                .toByteArray();
                        message.setPayload(dataBytes);
                        dataBytes = message.getDataBytes();
                        break;
                    }
                    case CACHE_META_SUCCESS_BYTES: {
                        message.setPayload(SUCCESS_BYTES);
                        dataBytes = message.getDataBytes();
                        break;
                    }
                    default:
                        dataBytes = cachedMessageVal.toByteArray();
                    }
                    send(dataBytes, message.getAddress(), message.getPort());
                    return;
                } 
                else {
                    try {
                        if (!mMessageCache.putIfNotExist(message.getIdString(),
                                MessageCache.ENTRY_BEING_PROCESSED, CACHE_META_COMPLETE_RESPONSE, 0)) {
                            // Message is being processed by other thread so move on.
                            return;
                        }
                    } catch (OutOfMemoryError e) {
                        sendOverloadMessage(message, kvResBuilder);
                        return;
                    }
                }

                kvReqBuilder = kvReqBuilder.mergeFrom(message.getPayload());

                key = kvReqBuilder.getKey();
                value = kvReqBuilder.getValue();
                
                switch (kvReqBuilder.getCommand()) {
                case Protocol.PUT: {
                    if (key.isEmpty() || key.size() > Protocol.SIZE_MAX_KEY_LENGTH) {
                        errCode = Protocol.ERR_INVALID_KEY;
                    } else if (value.size() > Protocol.SIZE_MAX_VAL_LENGTH) {
                        errCode = Protocol.ERR_INVALID_VAL;
                    } else {
                        VirtualNode vnode = mHashEntity.getKVNode(key);
                        if (kvReqBuilder.getIsReplica()) {
                            mKeyValStore.put(key, value, kvReqBuilder.getVersion(), VectorClock.getVectorClock(kvReqBuilder));
                            dataBytes = SUCCESS_BYTES;
                            cacheMetaInfo = CACHE_META_SUCCESS_BYTES | MessageCache.META_MASK_CACHE_REFERENCE;
                        } else if(vnode.getPNodeId() != mNodeId) {
                            routeToNode(message, vnode.getPNodeId());
                            // message being processed by other node, move on
                            return;
                        } else {
                            ValuePair curEntry = mKeyValStore.put(key, value, kvReqBuilder.getVersion(), VectorClock.getVectorClock(kvReqBuilder));
                            dataBytes = SUCCESS_BYTES;
                            cacheMetaInfo = CACHE_META_SUCCESS_BYTES | MessageCache.META_MASK_CACHE_REFERENCE;
                            
                            if (Protocol.REPLICATION_FACTOR > 1) {
                                kvReqBuilder
                                    .setIsReplica(true)
                                    .setValue(curEntry.value)
                                    .setVersion(curEntry.version);

                                kvReqBuilder.clearVectorClock();
                                for(int i = 0; i < curEntry.vectorClock.length; i++) {
                                    kvReqBuilder.addVectorClock(curEntry.vectorClock[i]);
                                }
                                
                                message.setPayload(kvReqBuilder.build().toByteArray());
                                
                                int[] successorNodeIds = new int[Protocol.REPLICATION_FACTOR - 1];
                                int numVNodes = mHashEntity.getSuccessorNodes(vnode, Protocol.REPLICATION_FACTOR - 1, successorNodeIds);
                                
                                for (int i = 0; i < numVNodes; i++) {
                                    routeToReplicaNode(message, successorNodeIds[i]);
                                }
                            }
                        }
                    }
                    break;
                }
                case Protocol.GET: {
                    if (key.isEmpty() || key.size() > Protocol.SIZE_MAX_KEY_LENGTH) {
                        errCode = Protocol.ERR_INVALID_KEY;
                    } else if (!value.isEmpty()) {
                        errCode = Protocol.ERR_INVALID_VAL;
                    } else {
                        int nodeId = mHashEntity.getKVNodeId(key);
                        if(nodeId != mNodeId) {
                            if (mMigrateKVHandler.isMigrating(nodeId)) {
                                sendOverloadMessage(message, kvResBuilder);
                                return;
                            }
                            routeToNode(message, nodeId);
                            // message being processed by other node, move on
                            return;
                        }
                        else {
                            vPair = mKeyValStore.get(key);
                            if (vPair != null) {
                            	dataBytes = kvResBuilder
                                        .setErrCode(Protocol.ERR_SUCCESS)
                                        .setValue(vPair.value)
                                        .setVersion(vPair.version)
                                        .build()
                                        .toByteArray();
                                cacheMetaInfo = CACHE_META_SUCCESS_GET | MessageCache.META_MASK_CACHE_REFERENCE;
                            } else {
                                errCode = Protocol.ERR_NON_EXISTENT_KEY;
                            }
                        }
                    }
                    break;
                }
                case Protocol.REMOVE: {
                    if (key.isEmpty() || key.size() > Protocol.SIZE_MAX_KEY_LENGTH) {
                        errCode = Protocol.ERR_INVALID_KEY;
                    } else if (!value.isEmpty()) {
                        errCode = Protocol.ERR_INVALID_VAL;
                    } else {
                        int nodeId = mHashEntity.getKVNodeId(key);
                        if(nodeId != mNodeId) {
                            if (mMigrateKVHandler.isMigrating(nodeId)) {
                                sendOverloadMessage(message, kvResBuilder);
                                return;
                            }
                            routeToNode(message, nodeId);
                            // message being processed by other node, move on
                            return;
                        }
                        else {
                            if (mKeyValStore.remove(key)) {
                                dataBytes = SUCCESS_BYTES;
                                cacheMetaInfo = CACHE_META_SUCCESS_BYTES | MessageCache.META_MASK_CACHE_REFERENCE;
                            } else {
                                errCode = Protocol.ERR_NON_EXISTENT_KEY;
                            }
                        }
                    }
                    break;
                }
                case Protocol.SHUTDOWN:
                    System.exit(0);
                    break;
                case Protocol.WIPEOUT:
                    if (mMigrateKVHandler.isMigrating()){
                        System.err.println("[ERROR]: Overload during wipeout");
                        sendOverloadMessage(message, kvResBuilder);
                        // message overload because of migration, move on
                        return;
                    } 
                    mKeyValStore.removeAll();                        
                    dataBytes = SUCCESS_BYTES;
                    cacheMetaInfo = CACHE_META_SUCCESS_BYTES | MessageCache.META_MASK_CACHE_REFERENCE;
                    break;
                case Protocol.IS_ALIVE:
                    dataBytes = SUCCESS_BYTES;
                    cacheMetaInfo = CACHE_META_SUCCESS_BYTES | MessageCache.META_MASK_CACHE_REFERENCE;
                    break;
                case Protocol.GET_PID: {
                    String vmName = ManagementFactory.getRuntimeMXBean().getName();
                    int p = vmName.indexOf("@");
                    int pid = Integer.valueOf(vmName.substring(0, p));
                    dataBytes = kvResBuilder
                            .setErrCode(Protocol.ERR_SUCCESS)
                            .setPid(pid)
                            .build()
                            .toByteArray();
                    break;
                }
                case Protocol.GET_MEMBERSHIP_COUNT:
                    dataBytes = kvResBuilder
                    .setErrCode(Protocol.ERR_SUCCESS)
                    .setMembershipCount(EpidemicProtocol.getInstance().getAliveMembers())
                    .build()
                    .toByteArray();
                    break;
                default:
                    errCode = Protocol.ERR_UNRECOGNIZED_COMMAND;
                    break;
                }
                
            } catch (OutOfMemoryError e) {
                errCode = Protocol.ERR_OUT_OF_SPACE;
            } catch (Exception e) {
                e.printStackTrace();
                errCode = Protocol.ERR_INTERNAL_KVSTORE_FAILURE;
            }
            
            if (errCode != Protocol.ERR_SUCCESS) {
                dataBytes = kvResBuilder
                        .setErrCode(errCode)
                        .build()
                        .toByteArray();
            }

            message.setPayload(dataBytes);
            dataBytes = message.getDataBytes();
            
            metaInfo = cacheMetaInfo & 0x0000ffff;
            switch(metaInfo) {
            case CACHE_META_SUCCESS_GET:
                mMessageCache.put(message.getIdString(), kvResBuilder.getValue(), cacheMetaInfo, kvResBuilder.getVersion());
                break;
            case CACHE_META_SUCCESS_BYTES:
                mMessageCache.put(message.getIdString(), null, cacheMetaInfo, 0);
                break;
            default:
                mMessageCache.put(message.getIdString(), ByteString.copyFrom(dataBytes), cacheMetaInfo, 0);
            }
            
            send(dataBytes, message.getAddress(), message.getPort());
            
            if (kvReqBuilder.hasReplyIpAddress() && kvReqBuilder.hasReplyPort()) {
                send(dataBytes, InetAddress.getByName(kvReqBuilder.getReplyIpAddress()), kvReqBuilder.getReplyPort());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private void routeToNode(NetworkMessage message, int nodeId) throws Exception {
        kvReqBuilder.setReplyIpAddress(message.getAddress().getHostAddress());
        kvReqBuilder.setReplyPort(message.getPort());
        message.setPayload(kvReqBuilder.build().toByteArray());
        AddressHolder fromAddress = new AddressHolder(message.getAddress(), message.getPort());
        
        AddressHolder routedNode = mRouteStrat.getRoute(nodeId);
        
        if (routedNode == null) {
            System.out.println(String.format("[DEBUG]: Unable to route to nodeId %d", nodeId));
            return;
        }
        
        message.setAddressAndPort(routedNode.address, routedNode.port);
        mKVClient.send(message, fromAddress);
    }
    
    private void sendOverloadMessage(NetworkMessage message, KVResponse.Builder kvResBuilder) throws IOException, InterruptedException {
        message.setPayload(kvResBuilder
                .setErrCode(Protocol.ERR_SYSTEM_OVERLOAD)
                .setOverloadWaitTime(Protocol.getOverloadWaittime())
                .build()
                .toByteArray());
        byte[] dataBytes = message.getDataBytes();
        send(dataBytes, message.getAddress(), message.getPort());
    }
    
    private void routeToReplicaNode(NetworkMessage message, int nodeId) throws Exception {
        if (nodeId == mNodeId) {
            return;
        }
        // Each replica message must be independent from another, since they are sent to different addresses.
        NetworkMessage replicaMsg = NetworkMessage.clone(message, Util.getUniqueId(ReactorServer.KEY_VALUE_PORT));
        
        AddressHolder routedNode = mRouteStrat.getRoute(nodeId);
        replicaMsg.setAddressAndPort(routedNode.address, routedNode.port);
        mSecondaryKVClient.send(replicaMsg, null);
    }
    
    private void send(byte[] dataBytes, InetAddress addr, int port) throws IOException, InterruptedException {
        ByteBuffer buf = ByteBuffer.wrap(dataBytes);
        WriteEventHandler.write(mChannel, buf, new InetSocketAddress(addr, port));
    }
}
