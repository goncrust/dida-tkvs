package dadkvs.server;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

import dadkvs.DadkvsPaxosServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

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

    LinkedHashMap<Integer, PendingTransaction> pendingTransactions; // linked to prioritize insertion order
    Queue<PendingRequest> pendingRequests;

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
        pendingTransactions = new LinkedHashMap<>();
        pendingRequests = new LinkedList<>();

        currentIndex = new AtomicInteger(-1);
        rnd = new ArrayList<AtomicInteger>();
        vrnd = new ArrayList<AtomicInteger>();
        vval = new ArrayList<AtomicInteger>();
        newPaxos();

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
        currentIndex.getAndIncrement();
        System.out.println("newPaxos with currentIndex: " + currentIndex.get());
    }

}
