import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import rpc.UduckSrv;

public class DataHandler {
    public static TTransport getTransport() {
        TTransport transport = new TFramedTransport(new TSocket("localhost", 6980));
        return transport;
    }

    public static UduckSrv.Client getClient(TTransport transport) {
        UduckSrv.Client client = null;
        try {
            TBinaryProtocol protocol = new TBinaryProtocol(transport);
            client = new UduckSrv.Client(protocol);
            transport.open();
        } catch (TException e) {
            e.printStackTrace();
        }
        return client;
    }

    public static void citySortAndLoca(UduckSrv.Client client, String ip) {
        try {
            client.citySortAndLoca(ip);
        } catch (TException e) {
            e.printStackTrace();
        }
    }

    public static void destoryTransport(TTransport transport) {
        transport.close();
    }
}