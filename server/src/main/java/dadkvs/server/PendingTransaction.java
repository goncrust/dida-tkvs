package dadkvs.server;

import dadkvs.DadkvsMain;
import io.grpc.stub.StreamObserver;

public class PendingTransaction {
    private TransactionRecord transaction;
    private StreamObserver<DadkvsMain.CommitReply> responseObserver;

    public PendingTransaction(TransactionRecord transaction,
            StreamObserver<DadkvsMain.CommitReply> responseObserver) {
        this.transaction = transaction;
        this.responseObserver = responseObserver;
    }

    // Getter methods for all fields
    public TransactionRecord getTransaction() {
        return transaction;
    }

    public StreamObserver<DadkvsMain.CommitReply> getResponseObserver() {
        return responseObserver;
    }
}
