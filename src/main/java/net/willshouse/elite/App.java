package net.willshouse.elite;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;
/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        // Prepare our context and subscriber
        Context context = ZMQ.context(1);
        Socket subscriber = context.socket(ZMQ.SUB);

        subscriber.connect("tcp://firehose.elite-market-data.net:9050");
        subscriber.subscribe("".getBytes());
        //subscriber.subscribe(new byte[1]);
        while (!Thread.currentThread ().isInterrupted ()) {
            // Read envelope with address
            String data = subscriber.recvStr ();
            // Read message contents
            System.out.println(data);
        }
        subscriber.close ();
        context.term ();
    }
}
