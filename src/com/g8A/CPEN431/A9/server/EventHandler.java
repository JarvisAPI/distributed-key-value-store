package com.g8A.CPEN431.A9.server;

import java.nio.channels.SelectionKey;

public interface EventHandler {
    void handleEvent(SelectionKey key);
}
