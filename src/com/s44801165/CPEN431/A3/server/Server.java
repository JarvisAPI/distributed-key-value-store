package com.s44801165.CPEN431.A3.server;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.protobuf.ByteString;
import com.s44801165.CPEN431.A3.protocol.NetworkMessage;
import com.s44801165.CPEN431.A3.server.KeyValueStore.ValuePair;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse;
import com.s44801165.CPEN431.A3.protocol.Protocol;

public class Server {
    private DatagramSocket mSocket;
    private KeyValueStore mKeyValStore;

    private Server(int port) throws SocketException {
        mSocket = new DatagramSocket(port);
        mKeyValStore = KeyValueStore.getInstance();
    }

    private void runServer() {
        ValuePair vPair;
        
        final int SIZE_MAX_QUEUE = 10;
        BlockingQueue<NetworkMessage> queue = new LinkedBlockingQueue<>(SIZE_MAX_QUEUE);
        
        MessageProducer msgProducer = new MessageProducer(mSocket, queue);
        msgProducer.start();
        
        while (true) {
            try {
                byte[] dataBytes = null; 
                int errCode = Protocol.ERR_SUCCESS;
                NetworkMessage message = queue.take();
                try {
                    KeyValueRequest.KVRequest kvReq = KeyValueRequest.KVRequest.newBuilder()
                            .mergeFrom(message.getPayload()).build();
    
                    ByteString key = kvReq.getKey();
                    ByteString value = kvReq.getValue();
    
                    switch (kvReq.getCommand()) {
                    case Protocol.PUT: {
                        if (key.isEmpty() || key.size() > Protocol.SIZE_MAX_KEY_LENGTH) {
                            errCode = Protocol.ERR_INVALID_KEY;
                        } else if (value.size() > Protocol.SIZE_MAX_VAL_LENGTH) {
                            errCode = Protocol.ERR_INVALID_VAL;
                        } else {
                            mKeyValStore.put(key, value, kvReq.getVersion());
                            dataBytes = KeyValueResponse.KVResponse.newBuilder()
                                    .setErrCode(Protocol.ERR_SUCCESS).build()
                                    .toByteArray();
                        }
                        break;
                    }
                    case Protocol.GET: {
                        if (key.isEmpty() || key.size() > Protocol.SIZE_MAX_KEY_LENGTH) {
                            errCode = Protocol.ERR_INVALID_KEY;
                        } else if (!value.isEmpty()) {
                            errCode = Protocol.ERR_INVALID_VAL;
                        } else {
                            vPair = mKeyValStore.get(key);
                            if (vPair != null) {
                                dataBytes = KeyValueResponse.KVResponse.newBuilder()
                                        .setErrCode(Protocol.ERR_SUCCESS)
                                        .setValue(vPair.value)
                                        .setVersion(vPair.version)
                                        .build()
                                        .toByteArray();
                            }   else {
                                errCode = Protocol.ERR_NON_EXISTENT_KEY;
                            }
                        }
                        break;
                    }
                    case Protocol.REMOVE: {
                        if (key.isEmpty() || key.size() > Protocol.SIZE_MAX_KEY_LENGTH) {
                            errCode = Protocol.ERR_INVALID_KEY;
                        } else if (!value.isEmpty()) {
                            errCode = Protocol.ERR_INVALID_VAL;
                        } else if (mKeyValStore.remove(key)) {
                            dataBytes = KeyValueResponse.KVResponse.newBuilder()
                                    .setErrCode(Protocol.ERR_SUCCESS)
                                    .build()
                                    .toByteArray();
                        } else {
                            errCode = Protocol.ERR_NON_EXISTENT_KEY;
                        }
                        break;
                    }
                    case Protocol.SHUTDOWN:
                        System.exit(0);
                        break;
                    case Protocol.WIPEOUT:
                        mKeyValStore.removeAll();
                        dataBytes = KeyValueResponse.KVResponse.newBuilder()
                        .setErrCode(Protocol.ERR_SUCCESS)
                        .build()
                        .toByteArray();
                        break;
                    case Protocol.IS_ALIVE:
                        dataBytes = KeyValueResponse.KVResponse.newBuilder()
                        .setErrCode(Protocol.ERR_SUCCESS)
                        .build()
                        .toByteArray();
                        break;
                    case Protocol.GET_PID:
                        // get pid somehow.
                        break;
                    case Protocol.GET_MEMBERSHIP_COUNT:
                        dataBytes = KeyValueResponse.KVResponse.newBuilder()
                        .setErrCode(Protocol.ERR_SUCCESS)
                        .setMembershipCount(1)
                        .build()
                        .toByteArray();
                        break;
                    default:
                        errCode = Protocol.ERR_UNRECOGNIZED_COMMAND;
                        break;
                    }
                    
                } catch (Exception e) {
                    e.printStackTrace();
                    errCode = Protocol.ERR_INTERNAL_KVSTORE_FAILURE;
                }
                
                if (errCode != Protocol.ERR_SUCCESS) {
                    dataBytes = KeyValueResponse.KVResponse.newBuilder()
                            .setErrCode(errCode)
                            .build()
                            .toByteArray();
                }

                message.setPayload(dataBytes);
                dataBytes = message.getDataBytes();
                
                DatagramPacket reply = new DatagramPacket(
                        dataBytes,
                        dataBytes.length,
                        message.getAddress(),
                        message.getPort());
                
                mSocket.send(reply);

            } catch (IOException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        try {
            System.out.println("Starting server!");
            Server server = new Server(8082);
            server.runServer();
        } catch (SocketException e) {
            e.printStackTrace();
        }
    }
}
