package dadkvs.server;

import dadkvs.DadkvsPaxosServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class DadkvsServerState {
    boolean i_am_leader;
    int debug_mode;
    int base_port;
    int my_id;
    int store_size;
    int n_servers;
    int responses_needed;
    KeyValueStore store;
    MainLoop main_loop;
    Thread main_loop_worker;

    // TODO: Trocar estas data structures. Nao precisam de tolerar concorrencia
    // porque tudo no main loop e synchornized
    ConcurrentHashMap<Integer, PendingTransaction> pendingTransactions;
    LinkedBlockingQueue<PendingRequest> pendingRequests;

    // Paxos variables
    AtomicInteger currentIndex;
    ArrayList<AtomicInteger> rnd;
    ArrayList<AtomicInteger> vrnd;
    ArrayList<AtomicInteger> vval;

    // Possible server configurations
    Integer[][] configs = { { 0, 1, 2 }, { 1, 2, 3 }, { 2, 3, 4 } };

    String[] targets;
    ManagedChannel[] channels;
    DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub[] async_stubs;

    public DadkvsServerState(int kv_size, int port, int myself) {
        base_port = port;
        my_id = myself;
        i_am_leader = false;
        debug_mode = 0;
        store_size = kv_size;
        n_servers = 5;
        responses_needed = 2;
        store = new KeyValueStore(kv_size);
        main_loop = new MainLoop(this);
        main_loop_worker = new Thread(main_loop);
        main_loop_worker.start();
        pendingTransactions = new ConcurrentHashMap<>();
        pendingRequests = new LinkedBlockingQueue<>();

        currentIndex.set(0);
        rnd = new ArrayList<AtomicInteger>();
        vrnd = new ArrayList<AtomicInteger>();
        vval = new ArrayList<AtomicInteger>();

        targets = new String[n_servers];
        for (int i = 0; i < n_servers; i++) {
            targets[i] = new String();
            targets[i] = "localhost:" + (base_port + i);
        }
    }

    public void initComms() {
        // Let us use plaintext communication because we do not have certificates
        channels = new ManagedChannel[n_servers];

        for (int i = 0; i < n_servers; i++) {
            if (i != my_id)
                channels[i] = ManagedChannelBuilder.forTarget(targets[i]).usePlaintext().build();
        }

        async_stubs = new DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub[n_servers];

        for (int i = 0; i < n_servers; i++) {
            if (i != my_id)
                async_stubs[i] = DadkvsPaxosServiceGrpc.newStub(channels[i]);
        }
    }

    public void terminateComms() {
        for (int i = 0; i < n_servers; i++) {
            if (i != my_id)
                channels[i].shutdownNow();
        }
    }

    public void newPaxos() {
        rnd.add(new AtomicInteger(my_id));
        vrnd.add(new AtomicInteger(0));
        vval.add(new AtomicInteger(-1));
        currentIndex.getAndAdd(1);
    }

}
