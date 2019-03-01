package com.g8A.CPEN431.A8.server;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;

import com.g8A.CPEN431.A8.protocol.NetworkMessage;
import com.g8A.CPEN431.A8.protocol.Protocol;
import com.g8A.CPEN431.A8.server.MessageCache.CacheEntry;
import com.g8A.CPEN431.A8.server.distribution.DirectRoute;
import com.g8A.CPEN431.A8.server.distribution.EpidemicProtocol;
import com.g8A.CPEN431.A8.server.distribution.HashEntity;
import com.g8A.CPEN431.A8.server.distribution.RouteStrategy;
import com.g8A.CPEN431.A8.server.distribution.RouteStrategy.AddressHolder;
import com.google.protobuf.ByteString;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse.KVResponse;

public class KeyValueRequestTask implements Runnable {
    private static final int CACHE_META_COMPLETE_RESPONSE = 0;
    private static final int CACHE_META_SUCCESS_BYTES = 1;
    private static final int CACHE_META_SUCCESS_GET = 2;
    private byte[] mDataBytes;
    private KeyValueStore mKeyValStore;
    private MessageCache mMessageCache;
    private HashEntity mHashEntity;
    private RouteStrategy mRouteStrat;
    private int mNodeId;
    private DatagramChannel mChannel;
    private KeyValueRequest.KVRequest.Builder kvReqBuilder;
    private InetSocketAddress mAddr;

    public KeyValueRequestTask(DatagramChannel channel, InetSocketAddress addr, byte[] dataBytes) {
        mChannel = channel;
        mDataBytes = dataBytes;
        mKeyValStore = KeyValueStore.getInstance();
        mMessageCache = MessageCache.getInstance();
        mHashEntity = HashEntity.getInstance();
        mRouteStrat = DirectRoute.getInstance();
        mNodeId = DirectRoute.getInstance().getSelfNodeId();
        mAddr = addr;
    }

    @Override
    public void run() {
        try {   
            byte[] SUCCESS_BYTES = KeyValueResponse.KVResponse.newBuilder()
                    .setErrCode(Protocol.ERR_SUCCESS)
                    .build()
                    .toByteArray();
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
            
            DatagramPacket packet = new DatagramPacket(new byte[0], 0, null, 0);
    
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
                    packet.setData(dataBytes);
                    packet.setAddress(message.getAddress());
                    packet.setPort(message.getPort());
                    send(packet);
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
                        sendOverloadMessage(message, kvResBuilder, packet);
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
                        int nodeId = mHashEntity.getKVNodeId(key);
                        if(nodeId != mNodeId) {
                            routeToNode(message, nodeId);
                            // message being processed by other node, move on
                            return;
                        }
                        else {
                            mKeyValStore.put(key, value, kvReqBuilder.getVersion());
                            dataBytes = SUCCESS_BYTES;
                            cacheMetaInfo = CACHE_META_SUCCESS_BYTES | MessageCache.META_MASK_CACHE_REFERENCE;
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
                            if (MigrateKVThread.getInstance().isMigrating(nodeId)) {
                                sendOverloadMessage(message, kvResBuilder, packet);
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
                            if (MigrateKVThread.getInstance().isMigrating(nodeId)) {
                                sendOverloadMessage(message, kvResBuilder, packet);
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
                    if (MigrateKVThread.getInstance().isMigrating()){
                        sendOverloadMessage(message, kvResBuilder, packet);
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
                
            } catch (IllegalStateException e) {
                sendOverloadMessage(message, kvResBuilder, packet);
                return;
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
            
            packet.setData(dataBytes);
            packet.setAddress(message.getAddress());
            packet.setPort(message.getPort());
            
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
            
            send(packet);
            
            if (kvReqBuilder.hasReplyIpAddress() && kvReqBuilder.hasReplyPort()) {
                packet.setAddress(InetAddress.getByName(kvReqBuilder.getReplyIpAddress()));
                packet.setPort(kvReqBuilder.getReplyPort());
                send(packet);
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
        message.setAddressAndPort(routedNode.address, routedNode.port);
        ReactorServer.getInstance().getKVClient().send(message, fromAddress);
    }
    
    private void sendOverloadMessage(NetworkMessage message, KVResponse.Builder kvResBuilder, DatagramPacket packet) throws IOException {
        message.setPayload(kvResBuilder
                .setErrCode(Protocol.ERR_SYSTEM_OVERLOAD)
                .setOverloadWaitTime(Protocol.getOverloadWaittime())
                .build()
                .toByteArray());
        byte[] dataBytes = message.getDataBytes();
        packet.setData(dataBytes);
        packet.setAddress(message.getAddress());
        packet.setPort(message.getPort());
        send(packet);
    }
    
    private void send(DatagramPacket packet) throws IOException {
        ByteBuffer buf = ByteBuffer.wrap(packet.getData());
        mChannel.send(buf, new InetSocketAddress(packet.getAddress(), packet.getPort()));
    }
}
