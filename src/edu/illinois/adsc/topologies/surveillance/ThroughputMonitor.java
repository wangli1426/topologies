package edu.illinois.adsc.topologies.surveillance;

import backtype.storm.utils.RateTracker;
import edu.illinois.adsc.topologies.generated.SurveillanceService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * Created by robert on 10/29/15.
 */
public class ThroughputMonitor implements Runnable {

    public RateTracker rateTracker;

    private TTransport transport;

    private TProtocol protocol;

    private SurveillanceService.Client client;

    private String executorID;

    private Thread reportThread;

    public ThroughputMonitor(String executorId) {

        rateTracker = new RateTracker(5000,10);
        executorID = executorId;
        reportThread = new Thread(this);
        reportThread.start();
    }

    public void close() {
        reportThread.interrupt();
    }

    public boolean connectToServer() {


        transport = new TSocket(Surveillant.thriftIp, Surveillant.thriftPort);

        try {
            transport.open();
        } catch (TTransportException e) {
            e.printStackTrace();
            return false;
        }

        protocol = new TBinaryProtocol(transport);

        client  = new SurveillanceService.Client(protocol);

        return true;
    }


    @Override
    public void run() {
        while(!connectToServer()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        while(true) {
            try {
                Thread.sleep(1000);
                client.reportExecutorThroughput(executorID,rateTracker.reportRate());
            } catch (TException e ) {
                e.printStackTrace();
                connectToServer();
            } catch (Exception ee) {
                ee.printStackTrace();
            }
        }

    }
}
