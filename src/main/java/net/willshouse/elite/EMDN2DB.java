package net.willshouse.elite;

import org.apache.log4j.Logger;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * EMDN2DB
 */
public class EMDN2DB {
    static final Thread mainThread = Thread.currentThread();
    public static Logger log = Logger.getLogger(EMDN2DB.class);
    private static Context context;

    public static void main(String[] args) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Shutdown detected");
                context.term();
                try {
                    mainThread.interrupt();
                    mainThread.join();
                } catch (InterruptedException e) {

                }
            }
        });
        log.info("Starting Up...");
        // Prepare our context and subscriber
        String message;
        context = ZMQ.context(1);
        Socket subscriber = context.socket(ZMQ.SUB);
        ItemPriceRecord priceInfo;
        Connection conn = null;


        try {
            conn = setupDB();
        } catch (SQLException e) {
            log.fatal(e.toString(), e);
            System.exit(1);
        } catch (ClassNotFoundException e) {
            log.fatal(e.toString(), e);
            System.exit(1);
        }

        subscriber.connect("tcp://firehose.elite-market-data.net:9050");
        // a subscription socket MUST subscribe to something.  Even if in our case its ""
        subscriber.subscribe("".getBytes());
        log.info("Starting Listener");
        while (!mainThread.isInterrupted()) {
            log.debug("Waiting for Message");
            // this is a blocking call. waiting for message
            try {
                message = subscriber.recvStr();
                priceInfo = new ItemPriceRecord(message);
                //dump out data for now
                log.debug(message);
                try {
                    priceInfo.updateDB(conn);
                } catch (SQLException e) {
                    log.error(e.toString(), e);

                }
            } catch (ZMQException e) {
                if (e.getErrorCode() == ZMQ.Error.ETERM.getCode()) {
                    log.info("Stopping Listener");
                    break;
                } else {
                    log.error(e.toString(), e);
                }
            }
        }

        subscriber.close();
        log.info("Listener Stopped");
        try {
            log.info("Closing DB connection");
            conn.close();
        } catch (SQLException e) {

            log.error("Problem closing the DB:" + e.toString(), e);
        }
        log.info("ShutDown Completed");
        System.exit(0);
    }


    private static Connection setupDB() throws SQLException, ClassNotFoundException {
        log.info("Connecting to DB");
        Class.forName("org.h2.Driver");
        Connection conn = DriverManager.
                getConnection("jdbc:h2:tcp://localhost/~/test;IGNORECASE=TRUE", "sa", "");
        log.info("Connected");
        return conn;
    }

}
