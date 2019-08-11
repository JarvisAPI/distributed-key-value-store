package com.dkvstore;

public class Protocol {
    public static final int SIZE_MAX_KEY_LENGTH = 32;
    public static final int SIZE_MAX_VAL_LENGTH = 10000; // in bytes.
    
    public static final int PUT = 0x01;
    public static final int GET = 0x02;
    public static final int REMOVE = 0x03;
    public static final int SHUTDOWN = 0x04;
    public static final int WIPEOUT = 0x05;
    public static final int IS_ALIVE = 0x06;
    public static final int GET_PID = 0x07;
    public static final int GET_MEMBERSHIP_COUNT = 0x08;

    public static final int ERR_SUCCESS = 0x00;
    public static final int ERR_NON_EXISTENT_KEY = 0x01;
    public static final int ERR_OUT_OF_SPACE = 0x02;
    public static final int ERR_SYSTEM_OVERLOAD = 0x03;
    public static final int ERR_INTERNAL_KVSTORE_FAILURE = 0x04;
    public static final int ERR_UNRECOGNIZED_COMMAND = 0x05;
    public static final int ERR_INVALID_KEY = 0x06;
    public static final int ERR_INVALID_VAL = 0x07;
    
    public static int REPLICATION_FACTOR = 3;
    
    private static final int INIT_OVERLOAD_WAITTIME = 100;

    public static final int getOverloadWaittime() {
        return INIT_OVERLOAD_WAITTIME;
    }
}
