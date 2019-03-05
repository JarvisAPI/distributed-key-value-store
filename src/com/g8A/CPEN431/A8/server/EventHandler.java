package com.g8A.CPEN431.A8.server;

import java.nio.channels.SelectionKey;

public interface EventHandler {
    void handleEvent(SelectionKey key);
}
