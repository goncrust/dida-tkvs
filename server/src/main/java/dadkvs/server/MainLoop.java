package dadkvs.server;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import dadkvs.DadkvsMain;
import dadkvs.DadkvsPaxos;
import dadkvs.util.CollectorStreamObserver;
import dadkvs.util.GenericResponseCollector;
import io.grpc.stub.StreamObserver;

public class MainLoop implements Runnable {
    DadkvsServerState server_state;

    int backoff;
    boolean transactionNotAvailable;

    public MainLoop(DadkvsServerState state) {
        this.server_state = state;
        this.backoff = 100;
        transactionNotAvailable = false;
    }

    public void run() {
        while (true)
            this.doWork();
    }

    synchronized public void doWork() {
        System.out.println("Main loop do work start");
        while ((transactionNotAvailable || this.server_state.pendingRequests.isEmpty()) && (!this.server_state.i_am_leader
                || this.server_state.pendingTransactions.isEmpty())) {
            System.out.println("Main loop do work: waiting");
            try {
                wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }

        if (!this.server_state.pendingRequests.isEmpty()) {
            System.out.println("doWork: going to processPendingRequests");
            processPendingRequest();
        }

        if (this.server_state.i_am_leader && !this.server_state.pendingTransactions.isEmpty()) {
            System.out.println("doWork: going to proposePendingTransaction");
            proposePendingTransaction();
        }

        System.out.println("Main loop do work finish");
    }

    synchronized public void wakeup() {
        notify();
    }

    synchronized private void processPendingRequest() {
        System.out.println("------------- processPendingRequest -----------------");
        PendingRequest pendingRequest = this.server_state.pendingRequests.peek();

        if (!this.server_state.pendingTransactions.containsKey(pendingRequest.getReqid())) {
            transactionNotAvailable = true;
            return;
        }
        transactionNotAvailable = false;

        PendingTransaction pendingTransaction =
                this.server_state.pendingTransactions.get(pendingRequest.getReqid());
        pendingTransaction.getTransaction().setTimestamp(pendingRequest.getIndex());

        boolean result = this.server_state.store.commit(pendingTransaction.getTransaction());

        // for debug purposes
        System.out.println("Result ( " + result + " ) is ready for request with reqid "
                + pendingRequest.getReqid());

        DadkvsMain.CommitReply response = DadkvsMain.CommitReply.newBuilder()
                .setReqid(pendingRequest.getReqid()).setAck(result).build();

        StreamObserver<DadkvsMain.CommitReply> responseObserver =
                pendingTransaction.getResponseObserver();

        responseObserver.onNext(response);
        responseObserver.onCompleted();

        // Removing the already processed reqid from the queue.
        this.server_state.pendingRequests.poll();
        // Removing the already processed transaction from the queue.
        this.server_state.pendingTransactions.remove(pendingRequest.getReqid());
        System.out.println("------------- processPendingRequest end -----------------");
    }

    synchronized private void proposePendingTransaction() {
        System.out.println("------------- proposePendingTransaction -----------------");
        int proposingReqID = (Integer) this.server_state.pendingTransactions.keySet().toArray()[0];
        int proposedReqID = paxos(proposingReqID);
        if (proposedReqID == -1) {
            this.server_state.rnd.get(this.server_state.currentIndex.get())
                    .getAndAdd(this.server_state.n_servers);

            try {
                // if there are 2 concurrent leaders, this exponential backoff prevents them
                // from fighting indefinitely
                Thread.sleep(this.backoff);
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.exit(1);
            }
            this.backoff *= 2;
        } else {
            this.server_state.pendingRequests
                    .add(new PendingRequest(proposedReqID, this.server_state.currentIndex.get()));
            this.server_state.newPaxos();
            this.backoff = 100;
        }
        System.out.println("------------- proposePendingTransaction end -----------------");
    }

    private int paxos(int reqid) {
        // for debug purposes
        System.out.println("------------- paxos -----------------");
        System.out.println("Starting Paxos for reqid " + reqid);

        int config = this.server_state.store.read(0).getValue();
        int index = this.server_state.currentIndex.get();
        AtomicInteger rnd = this.server_state.rnd.get(index);

        if (rnd.get() == -1) {
            rnd.set(this.server_state.my_id);
        } else if (rnd.get() % this.server_state.n_servers != this.server_state.my_id) {
            int offset = rnd.get() % this.server_state.n_servers;
            int newRnd = rnd.get() + (offset - this.server_state.n_servers);
            rnd.set(newRnd < 0 ? newRnd + this.server_state.n_servers : newRnd);
        }

        // for debug purposes
        System.out.println("Starting Paxos Phase 1");
        DadkvsPaxos.PhaseOneRequest.Builder phase1_request =
                DadkvsPaxos.PhaseOneRequest.newBuilder();

        phase1_request.setPhase1Config(config);
        phase1_request.setPhase1Index(index);
        phase1_request.setPhase1Timestamp(this.server_state.rnd.get(index).get());

        ArrayList<DadkvsPaxos.PhaseOneReply> phase1_replies =
                new ArrayList<DadkvsPaxos.PhaseOneReply>();

        GenericResponseCollector<DadkvsPaxos.PhaseOneReply> phase1_collector =
                new GenericResponseCollector<DadkvsPaxos.PhaseOneReply>(phase1_replies,
                        this.server_state.n_servers);

        for (int i : this.server_state.configs[config]) {
            if (i == this.server_state.my_id)
                continue;

            CollectorStreamObserver<DadkvsPaxos.PhaseOneReply> phase1_observer =
                    new CollectorStreamObserver<DadkvsPaxos.PhaseOneReply>(phase1_collector);

            this.server_state.async_stubs[i].phaseone(phase1_request.build(), phase1_observer);
        }
        System.out.println("Sent phase1 request: config: " + config + "index" + index + "timestamp"
                + this.server_state.rnd.get(index).get() + ". Waiting for "
                + this.server_state.responses_needed);
        phase1_collector.waitForTarget(this.server_state.responses_needed);

        int agreedReqID = reqid;
        int acceptedRequests = 1;
        int maxRnd = -1;
        for (DadkvsPaxos.PhaseOneReply reply : phase1_replies) {
            if (reply.getPhase1Accepted()) {
                acceptedRequests++;
                int replyRnd = reply.getPhase1Timestamp();
                int replyVValue = reply.getPhase1Value();
                if (replyRnd > maxRnd && replyVValue != -1) {
                    agreedReqID = replyVValue;
                    maxRnd = replyRnd;
                }
            }
        }
        if (acceptedRequests < this.server_state.responses_needed) {
            // Phase 1 failed, we couldn't get a majority
            System.out.println("Phase1 failed to get majority");
            return -1;
        }

        // for debug purposes
        System.out.println("Startin Paxos Phase 2");

        // The leader is also an acceptor. Voting for the value we proposed
        this.server_state.vval.get(index).set(agreedReqID);

        DadkvsPaxos.PhaseTwoRequest.Builder phase2_request =
                DadkvsPaxos.PhaseTwoRequest.newBuilder();

        phase2_request.setPhase2Config(config);
        phase2_request.setPhase2Index(index);
        phase2_request.setPhase2Value(agreedReqID);
        phase2_request.setPhase2Timestamp(this.server_state.rnd.get(index).get());

        ArrayList<DadkvsPaxos.PhaseTwoReply> phase2_replies =
                new ArrayList<DadkvsPaxos.PhaseTwoReply>();

        GenericResponseCollector<DadkvsPaxos.PhaseTwoReply> phase2_collector =
                new GenericResponseCollector<DadkvsPaxos.PhaseTwoReply>(phase2_replies,
                        this.server_state.n_servers);

        for (int i : this.server_state.configs[config]) {
            if (i == this.server_state.my_id)
                continue;

            CollectorStreamObserver<DadkvsPaxos.PhaseTwoReply> phase2_observer =
                    new CollectorStreamObserver<DadkvsPaxos.PhaseTwoReply>(phase2_collector);

            this.server_state.async_stubs[i].phasetwo(phase2_request.build(), phase2_observer);
        }
        System.out.println(
                "Sent phase2 request: " + phase2_request + ". agreedReqID: " + agreedReqID);

        // Paxos was successful. The next ReqId to be processed has been decided.
        System.out.println("------------- paxos end -----------------");
        return agreedReqID;
    }
}
