package dadkvs.server;

import java.util.ArrayList;

import dadkvs.DadkvsPaxos;
import dadkvs.DadkvsPaxosServiceGrpc;
import dadkvs.util.CollectorStreamObserver;
import dadkvs.util.GenericResponseCollector;
import io.grpc.stub.StreamObserver;

public class DadkvsPaxosServiceImpl extends DadkvsPaxosServiceGrpc.DadkvsPaxosServiceImplBase {

    DadkvsServerState server_state;

    public DadkvsPaxosServiceImpl(DadkvsServerState state) {
        this.server_state = state;
    }

    @Override
    public void phaseone(DadkvsPaxos.PhaseOneRequest request,
            StreamObserver<DadkvsPaxos.PhaseOneReply> responseObserver) {
        // for debug purposes
        System.out.println("------------- phase1 -----------------");
        System.out.println("Receive phase1 request: " + request);

        DadkvsPaxos.PhaseOneReply response;

        int timestamp = request.getPhase1Timestamp();
        int config = request.getPhase1Config();
        int index = request.getPhase1Index();

        PaxosInstance inst = this.server_state.getPaxos(index);

        System.out.println("index received: " + index + " currentIndex: "
                + this.server_state.currentIndex.get() + " timestamp received: " + timestamp
                + " current timestamp (rnd): " + inst.getRnd());
        if (timestamp >= inst.getRnd()) {
            // Setting up the config to be used for this Paxos instance
            VersionedValue old_config = server_state.store.read(0);
            VersionedValue new_config = new VersionedValue(config, old_config.getVersion() + 1);
            server_state.store.write(0, new_config);

            inst.setRnd(timestamp);
            response = DadkvsPaxos.PhaseOneReply.newBuilder().setPhase1Config(config)
                    .setPhase1Index(index)
                    .setPhase1Timestamp(inst.getVrnd())
                    .setPhase1Accepted(true).setPhase1Value(inst.getVval())
                    .build();

            System.out.println("Responded to phase one request with: index: " + index
                    + " timestamp: " + inst.getVrnd()
                    + "accepted: True value:" + inst.getVval());
        } else {
            response = DadkvsPaxos.PhaseOneReply.newBuilder().setPhase1Config(config)
                    .setPhase1Index(index)
                    .setPhase1Timestamp(inst.getRnd())
                    .setPhase1Accepted(false).build();
            System.out.println("Responded to phase one request with: index: " + index
                    + " timestamp: " + inst.getRnd() + "accepted: False");
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
        System.out.println("------------- phase1 end -----------------");
    }

    @Override
    public void phasetwo(DadkvsPaxos.PhaseTwoRequest request,
            StreamObserver<DadkvsPaxos.PhaseTwoReply> responseObserver) {
        // for debug purposes
        System.out.println("------------- phase2 -----------------");
        System.out.println("Receive phase two request: " + request);

        DadkvsPaxos.PhaseTwoReply response;

        int timestamp = request.getPhase2Timestamp();
        int config = request.getPhase2Config();
        int index = request.getPhase2Index();
        int value = request.getPhase2Value();

        PaxosInstance inst = this.server_state.getPaxos(index);

        System.out.println("index received: " + index + " currentIndex: "
                + this.server_state.currentIndex.get() + " timestamp received: " + timestamp
                + " current timestamp (rnd): " + inst.getRnd());
        if (timestamp >= inst.getRnd()) {

            inst.setRnd(timestamp);
            inst.setVrnd(timestamp);
            inst.setVval(value);

            response = DadkvsPaxos.PhaseTwoReply.newBuilder().setPhase2Config(config)
                    .setPhase2Index(index).setPhase2Accepted(true).build();
            System.out.println("Responded to phase two request with: config: " + config + " index: "
                    + index + "accepted: " + true);

            DadkvsPaxos.LearnRequest.Builder learn_request = DadkvsPaxos.LearnRequest.newBuilder();
            learn_request.setLearnconfig(config).setLearnindex(index)
                    .setLearnvalue(inst.getVval())
                    .setLearntimestamp(inst.getVrnd());

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
                System.out.println(
                        "Learn request sent to server " + j + " with: config " + config + " index "
                                + index + "learnvalue " + inst.getVval()
                                + " learntimestamp " + inst.getVrnd());
            }
            learn_collector.waitForTarget(this.server_state.responses_needed);
        } else {
            response = DadkvsPaxos.PhaseTwoReply.newBuilder().setPhase2Config(config)
                    .setPhase2Index(index).setPhase2Accepted(false).build();
            System.out.println("Responded to phase two request with: config: " + config + " index: "
                    + index + "accepted: " + false);
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
        System.out.println("------------- phase2 end -----------------");
    }

    @Override
    public void learn(DadkvsPaxos.LearnRequest request,
            StreamObserver<DadkvsPaxos.LearnReply> responseObserver) {
        // for debug purposes
        System.out.println("------------- learn -----------------");
        System.out.println("Received learn request: " + request);

        DadkvsPaxos.LearnReply response;

        int value = request.getLearnvalue();
        int config = request.getLearnconfig();
        int index = request.getLearnindex();

        PendingRequest newRequest = new PendingRequest(value, index);

        System.out.println("currentindex " + this.server_state.currentIndex.get()
                + " received index " + index);

        if (index >= this.server_state.currentIndex.get()) {
            this.server_state.pendingRequests.add(newRequest);

            // Waking up the main loop since we may have a new request to work on.
            this.server_state.main_loop.wakeup();

            // TODO were should we increment the current round
        }

        response = DadkvsPaxos.LearnReply.newBuilder().setLearnaccepted(true)
                .setLearnindex(index).setLearnconfig(config).build();
        System.out.println("Responded to learn request with: accepted: true index: " + index
                + " config: " + config);

        responseObserver.onNext(response);
        responseObserver.onCompleted();
        System.out.println("------------- learn end -----------------");
    }
}
