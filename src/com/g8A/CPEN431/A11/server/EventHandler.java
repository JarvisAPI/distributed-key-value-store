package com.g8A.CPEN431.A11.server;

import java.nio.channels.SelectionKey;

public interface EventHandler {
    void handleEvent(SelectionKey key);
}
