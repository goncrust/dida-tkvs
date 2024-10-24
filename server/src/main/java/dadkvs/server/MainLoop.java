package dadkvs.server;

import java.util.ArrayList;

import dadkvs.DadkvsMain;
import dadkvs.DadkvsPaxos;
import dadkvs.util.CollectorStreamObserver;
import dadkvs.util.GenericResponseCollector;
import io.grpc.stub.StreamObserver;

public class MainLoop implements Runnable {
    DadkvsServerState server_state;
    int last_finished_index;
    boolean last_paxos_failed;

    public MainLoop(DadkvsServerState state) {
        this.server_state = state;
        this.last_finished_index = -1;
        this.last_paxos_failed = false;
    }

    public void run() {
        while (true)
            this.doWork();
    }

    private boolean checkNextRequestReady() {
        // cleanup repeated requests and check if nextRequestReady
        boolean nextRequestReady = false;
        while (!this.server_state.pendingRequests.isEmpty()) {
            PendingRequest pendingRequest = this.server_state.pendingRequests.peek();
            if (pendingRequest.getIndex() < this.server_state.currentIndex.get()) {
                this.server_state.pendingRequests.poll();
            } else {
                if (pendingRequest.getIndex() == this.server_state.currentIndex.get())
                    nextRequestReady = this.server_state.pendingTransactions
                            .containsKey(pendingRequest.getReqid());
                break;
            }
        }
        return nextRequestReady;
    }

    private boolean checkLeaderPendingTransaction() {
        // check if leader has pending transaction to propose
        return this.server_state.i_am_leader && !this.server_state.pendingTransactions.isEmpty()
                && last_finished_index != this.server_state.currentIndex.get();
    }

    private void checkReconfiguration() {
        if (this.server_state.nextConfigIndexes.isEmpty())
            return;

        int nextIndex = this.server_state.nextConfigIndexes.get(0);
        if (nextIndex == this.server_state.currentIndex.get()) {
            this.server_state.reconfigurePaxos();
            this.server_state.nextConfigIndexes.remove(0);
        }
    }

    synchronized public void doWork() {
        System.out.println("Main loop do work start");

        boolean nextRequestReady = checkNextRequestReady();
        boolean leaderPendingTransaction = checkLeaderPendingTransaction();

        while (!nextRequestReady && !leaderPendingTransaction) {
            System.out.println("Main loop do work: waiting");
            try {
                wait();
                nextRequestReady = checkNextRequestReady();
                leaderPendingTransaction = checkLeaderPendingTransaction();
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }

        if (nextRequestReady) {
            System.out.println("doWork: going to processPendingRequests");
            processPendingRequest();
        } else if (leaderPendingTransaction) {
            System.out.println("doWork: going to proposePendingTransaction");
            proposePendingTransaction();
        }

        checkReconfiguration();

        System.out.println("Main loop do work finish");
    }

    synchronized public void wakeup() {
        notify();
    }

    synchronized private void processPendingRequest() {
        System.out.println("------------- processPendingRequest -----------------");
        PendingRequest pendingRequest = this.server_state.pendingRequests.peek();

        if (pendingRequest.getIndex() != this.server_state.currentIndex.get())
            return;

        if (!this.server_state.pendingTransactions.containsKey(pendingRequest.getReqid()))
            return;

        PendingTransaction pendingTransaction = this.server_state.pendingTransactions.get(pendingRequest.getReqid());
        pendingTransaction.getTransaction().setTimestamp(pendingRequest.getIndex());

        TransactionRecord transaction = pendingTransaction.getTransaction();

        boolean result = true;

        // checking for reconfiguration
        if (transaction.getPrepareKey() == 0) {
            this.server_state.nextConfigIndexes.add(this.server_state.currentIndex.get() + this.server_state.padding);
        } else {
            // commit
            result = this.server_state.store.commit(transaction);
        }

        // increment current index
        this.server_state.currentIndex.getAndIncrement();
        this.last_paxos_failed = false;

        // for debug purposes
        System.out.println("Result ( " + result + " ) is ready for request with reqid "
                + pendingRequest.getReqid());

        DadkvsMain.CommitReply response = DadkvsMain.CommitReply.newBuilder()
                .setReqid(pendingRequest.getReqid()).setAck(result).build();

        StreamObserver<DadkvsMain.CommitReply> responseObserver = pendingTransaction.getResponseObserver();

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
            this.last_paxos_failed = true;
        } else {
            this.server_state.pendingRequests
                    .add(new PendingRequest(proposedReqID, this.server_state.currentIndex.get()));
            this.last_finished_index = this.server_state.currentIndex.get();
        }
        System.out.println("------------- proposePendingTransaction end -----------------");
    }

    private int paxos(int reqid) {
        // for debug purposes
        System.out.println("------------- paxos -----------------");

        int config = this.server_state.store.read(0).getValue();
        int index = this.server_state.currentIndex.get();

        PaxosInstance inst = server_state.getPaxos(index);
        int rnd = inst.getRnd();

        if (inst.getVval() != -1 && inst.getVval() != reqid)
            reqid = inst.getVval();

        System.out.println("Starting Paxos for reqid " + reqid);

        if (rnd == -1) {
            inst.setRnd(this.server_state.my_id);
        } else if (rnd % this.server_state.n_servers != this.server_state.my_id) {
            int offset = rnd % this.server_state.n_servers;
            int newRnd = rnd + (offset - this.server_state.n_servers);
            inst.setRnd(newRnd < 0 ? newRnd + this.server_state.n_servers : newRnd);
        } else if (last_paxos_failed) {
            inst.setRnd(inst.getRnd() + this.server_state.n_servers);
        }

        // for debug purposes
        System.out.println("Starting Paxos Phase 1");
        DadkvsPaxos.PhaseOneRequest.Builder phase1_request = DadkvsPaxos.PhaseOneRequest.newBuilder();

        phase1_request.setPhase1Config(config);
        phase1_request.setPhase1Index(index);
        phase1_request.setPhase1Timestamp(inst.getRnd());

        ArrayList<DadkvsPaxos.PhaseOneReply> phase1_replies = new ArrayList<DadkvsPaxos.PhaseOneReply>();

        GenericResponseCollector<DadkvsPaxos.PhaseOneReply> phase1_collector = new GenericResponseCollector<DadkvsPaxos.PhaseOneReply>(
                phase1_replies,
                this.server_state.n_servers);

        for (int i : this.server_state.configs[config]) {
            if (i == this.server_state.my_id)
                continue;

            CollectorStreamObserver<DadkvsPaxos.PhaseOneReply> phase1_observer = new CollectorStreamObserver<DadkvsPaxos.PhaseOneReply>(
                    phase1_collector);

            this.server_state.async_stubs[i].phaseone(phase1_request.build(), phase1_observer);
        }
        System.out.println(
                "Sent phase1 request: config: " + config + " index: " + index + " timestamp: "
                        + inst.getRnd() + ". Waiting for " + this.server_state.responses_needed);
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
        inst.setVrnd(inst.getRnd());
        inst.setVval(agreedReqID);

        DadkvsPaxos.PhaseTwoRequest.Builder phase2_request = DadkvsPaxos.PhaseTwoRequest.newBuilder();

        phase2_request.setPhase2Config(config);
        phase2_request.setPhase2Index(index);
        phase2_request.setPhase2Value(agreedReqID);
        phase2_request.setPhase2Timestamp(inst.getRnd());

        ArrayList<DadkvsPaxos.PhaseTwoReply> phase2_replies = new ArrayList<DadkvsPaxos.PhaseTwoReply>();

        GenericResponseCollector<DadkvsPaxos.PhaseTwoReply> phase2_collector = new GenericResponseCollector<DadkvsPaxos.PhaseTwoReply>(
                phase2_replies,
                this.server_state.n_servers);

        for (int i : this.server_state.configs[config]) {
            if (i == this.server_state.my_id)
                continue;

            CollectorStreamObserver<DadkvsPaxos.PhaseTwoReply> phase2_observer = new CollectorStreamObserver<DadkvsPaxos.PhaseTwoReply>(
                    phase2_collector);

            this.server_state.async_stubs[i].phasetwo(phase2_request.build(), phase2_observer);
        }
        System.out.println(
                "Sent phase2 request: " + phase2_request + ". agreedReqID: " + agreedReqID);

        phase2_collector.waitForTarget(this.server_state.responses_needed);

        acceptedRequests = 1;
        for (DadkvsPaxos.PhaseTwoReply reply : phase2_replies) {
            if (reply.getPhase2Accepted())
                acceptedRequests++;
        }
        if (acceptedRequests < this.server_state.responses_needed) {
            // Phase 2 failed, we couldn't get a majority
            System.out.println("Phase2 failed to get majority");
            return -1;
        }

        // The leader is also an acceptor. Sending learns to learners
        DadkvsPaxos.LearnRequest.Builder learn_request = DadkvsPaxos.LearnRequest.newBuilder();
        learn_request.setLearnconfig(config).setLearnindex(index).setLearnvalue(inst.getVval())
                .setLearntimestamp(inst.getRnd());

        ArrayList<DadkvsPaxos.LearnReply> learn_responses = new ArrayList<DadkvsPaxos.LearnReply>();

        GenericResponseCollector<DadkvsPaxos.LearnReply> learn_collector = new GenericResponseCollector<DadkvsPaxos.LearnReply>(
                learn_responses,
                this.server_state.n_servers);

        for (int j = 0; j < this.server_state.n_servers; j++) {
            if (j == this.server_state.my_id)
                continue;

            CollectorStreamObserver<DadkvsPaxos.LearnReply> learn_observer = new CollectorStreamObserver<DadkvsPaxos.LearnReply>(
                    learn_collector);
            this.server_state.async_stubs[j].learn(learn_request.build(), learn_observer);
            System.out.println("Learn request sent to server " + j + " with: config " + config
                    + " index " + index + "learnvalue " + inst.getVval() + " learntimestamp "
                    + inst.getRnd());
        }
        learn_collector.waitForTarget(0);

        // Paxos was successful. The next ReqId to be processed has been decided.
        System.out.println("------------- paxos end -----------------");
        return agreedReqID;
    }
}
