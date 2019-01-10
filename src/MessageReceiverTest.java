import static org.junit.Assert.*;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;

import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.s44801165.CPEN431.A2.MessageObserver;
import com.s44801165.CPEN431.A2.MessageObserver.MessageType;
import com.s44801165.CPEN431.A2.MessageReceiverThread;
import com.s44801165.CPEN431.A2.protocol.NetworkMessage;

import static org.mockito.Matchers.*;

public class MessageReceiverTest {
    private MessageReceiverThread mMessageReceiverThread;
    private DatagramSocket mSocket;
    private MessageObserver mMessageObserver;

    @Test
    public void unableToSendMessageShouldSignalTimeoutError() throws Exception {
        mSocket = Mockito.mock(DatagramSocket.class);
        mMessageReceiverThread = new MessageReceiverThread(mSocket);
        mMessageObserver = Mockito.mock(MessageObserver.class);
        
        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock arg0) throws Throwable {
                throw new IOException("IO exception test");
            }
        })
        .when(mSocket)
        .receive(Mockito.any(DatagramPacket.class));
        
        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock arg0) throws Throwable {
                mMessageReceiverThread.signalStop();
                return null;
            } 
        })
        .when(mMessageObserver)
        .update(Mockito.any(MessageType.class), Mockito.isNull());
        
        mMessageReceiverThread.attachMessageObserver(mMessageObserver);
        mMessageReceiverThread.run();
        
        Mockito.verify(mMessageObserver).update(Mockito.eq(MessageType.TIMEOUT), Mockito.isNull());
    }

}
