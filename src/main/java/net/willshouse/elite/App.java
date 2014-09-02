package net.willshouse.elite;

import org.apache.log4j.Logger;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * App
 *
 */
public class App 
{
    public static  Logger log = Logger.getLogger(App.class);
    public static void main( String[] args )
    {

        // Prepare our context and subscriber
        String message;
        Context context = ZMQ.context(1);
        Socket subscriber = context.socket(ZMQ.SUB);
        ItemPriceRecord priceInfo;
        Connection conn = null;

        try {
            conn = setupDB();
        } catch (SQLException e) {
            log.fatal(e.toString(),e);
            System.exit(1);
        } catch (ClassNotFoundException e) {
            log.fatal(e.toString(),e);
            System.exit(1);
        }

        subscriber.connect("tcp://firehose.elite-market-data.net:9050");
        // a subscription socket MUST subscribe to something.  Even if in our case its ""
        subscriber.subscribe("".getBytes());
        while (!Thread.currentThread ().isInterrupted ()) {
            // this is a blocking call. waiting for message
            message = subscriber.recvStr ();
            priceInfo = new ItemPriceRecord(message);
            //dump out data for now
            log.debug(message);
            try {
                priceInfo.updateDB(conn);
            } catch (SQLException e) {
                log.error(e.toString(), e);

            }
        }
        subscriber.close ();
        context.term ();
    }


    private static Connection setupDB() throws SQLException, ClassNotFoundException {

        Class.forName("org.h2.Driver");
        Connection conn = DriverManager.
                getConnection("jdbc:h2:tcp://localhost/~/test;IGNORECASE=TRUE", "sa", "");

        return conn;
    }

}
