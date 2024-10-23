package dadkvs.server;

import dadkvs.DadkvsMain;
import dadkvs.DadkvsMainServiceGrpc;
import io.grpc.stub.StreamObserver;
import java.util.Random;

public class DadkvsMainServiceImpl extends DadkvsMainServiceGrpc.DadkvsMainServiceImplBase {

    DadkvsServerState server_state;

    public DadkvsMainServiceImpl(DadkvsServerState state) {
        this.server_state = state;
    }

    @Override
    public void read(DadkvsMain.ReadRequest request,
            StreamObserver<DadkvsMain.ReadReply> responseObserver) {
        // for debug purposes
        System.out.println("------------- read -----------------");
        System.out.println("Receiving read request:" + request);

        if (this.server_state.is_freezed) {
            System.out.println("Server is freezed");
            return;
        }

        if (this.server_state.slow_mode) {
            // add a random delay
            int delay = new Random().nextInt(5000);
            System.out.println("Slow mode: delaying read request for " + delay + " ms");
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                System.out.println("InterruptedException");
            }
        }

        int reqid = request.getReqid();
        int key = request.getKey();
        VersionedValue vv = this.server_state.store.read(key);

        DadkvsMain.ReadReply response = DadkvsMain.ReadReply.newBuilder().setReqid(reqid)
                .setValue(vv.getValue()).setTimestamp(vv.getVersion()).build();

        System.out.println("Responded to read request with: reqid: " + reqid + " value: "
                + vv.getValue() + " timestamp: " + vv.getVersion());
        responseObserver.onNext(response);
        responseObserver.onCompleted();
        System.out.println("------------- read end -----------------");
    }

    @Override
    public void committx(DadkvsMain.CommitRequest request,
            StreamObserver<DadkvsMain.CommitReply> responseObserver) {
        // for debug purposes
        System.out.println("------------- committx -----------------");
        System.out.println("Receiving commit request:" + request);

        if (this.server_state.is_freezed) {
            System.out.println("Server is freezed");
            return;
        }

        if (this.server_state.slow_mode) {
            // add a random delay
            int delay = new Random().nextInt(1000);
            System.out.println("Slow mode: delaying read request for " + delay + " ms");
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                System.out.println("InterruptedException");
            }
        }

        int reqid = request.getReqid();
        int key1 = request.getKey1();
        int version1 = request.getVersion1();
        int key2 = request.getKey2();
        int version2 = request.getVersion2();
        int writekey = request.getWritekey();
        int writeval = request.getWriteval();

        // for debug purposes
        System.out.println("reqid " + reqid + " key1 " + key1 + " v1 " + version1 + " k2 " + key2
                + " v2 " + version2 + " wk " + writekey + " writeval " + writeval);

        TransactionRecord txrecord =
                new TransactionRecord(key1, version1, key2, version2, writekey, writeval);
        PendingTransaction transaction = new PendingTransaction(txrecord, responseObserver);

        this.server_state.pendingTransactions.put(reqid, transaction);

        // Waking up the main loop since the received transaction might have the req id
        // we decided to process next through consensus or, if the server is the leader,
        // propose it next.
        this.server_state.main_loop.wakeup();
        System.out.println("------------- committx end -----------------");
    }
}
