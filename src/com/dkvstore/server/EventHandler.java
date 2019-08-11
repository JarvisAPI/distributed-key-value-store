package com.dkvstore.server;

import java.nio.channels.SelectionKey;

public interface EventHandler {
    void handleEvent(SelectionKey key);
}
