package com.s44801165.CPEN431.A3.server;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.Arrays;

import com.google.protobuf.ByteString;
import com.s44801165.CPEN431.A3.protocol.NetworkMessage;
import com.s44801165.CPEN431.A3.protocol.Protocol;
import com.s44801165.CPEN431.A3.server.KeyValueStore.ValuePair;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse;

public class Server {
    private DatagramSocket mSocket;
    private KeyValueStore mKeyValStore;
    
    private static final byte[] SUCCESS_BYTES = KeyValueResponse.KVResponse.newBuilder()
            .setErrCode(Protocol.ERR_SUCCESS).build()
            .toByteArray();

    private Server(int port) throws SocketException {
        mSocket = new DatagramSocket(port);
        mKeyValStore = KeyValueStore.getInstance();
    }

    private void runServer() {
        byte[] maxDataBuf = NetworkMessage.getMaxDataBuffer();
        DatagramPacket packet = new DatagramPacket(maxDataBuf, maxDataBuf.length);
        NetworkMessage message = new NetworkMessage();
        ValuePair vPair;
        KeyValueResponse.KVResponse.Builder successResBuilder = KeyValueResponse.KVResponse.newBuilder()
                .setErrCode(Protocol.ERR_SUCCESS);

        byte[] dataBytes;
        int errCode;
        ByteString key;
        ByteString value;
        
        while (true) {
            try {
       
                mSocket.receive(packet);
                
                dataBytes = null; 
                errCode = Protocol.ERR_SUCCESS;
                
                try {
                    NetworkMessage.setMessage(message,
                            Arrays.copyOf(packet.getData(), packet.getLength()));
    
                    KeyValueRequest.KVRequest kvReq = KeyValueRequest.KVRequest.newBuilder()
                            .mergeFrom(message.getPayload()).build();
    
                    key = kvReq.getKey();
                    value = kvReq.getValue();
    
                    switch (kvReq.getCommand()) {
                    case Protocol.PUT: {
                        if (key.isEmpty() || key.size() > Protocol.SIZE_MAX_KEY_LENGTH) {
                            errCode = Protocol.ERR_INVALID_KEY;
                        } else if (value.size() > Protocol.SIZE_MAX_VAL_LENGTH) {
                            errCode = Protocol.ERR_INVALID_VAL;
                        } else {
                            mKeyValStore.put(key, value, kvReq.getVersion());
                            dataBytes = SUCCESS_BYTES;
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
                                dataBytes = successResBuilder
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
                            dataBytes = SUCCESS_BYTES;
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
                
                packet.setData(dataBytes);
                mSocket.send(packet);
                packet.setData(maxDataBuf);

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
