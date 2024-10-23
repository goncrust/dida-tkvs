package dadkvs.server;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

import dadkvs.DadkvsPaxosServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class DadkvsServerState {
    boolean i_am_leader;
    boolean is_freezed;
    boolean slow_mode;

    int base_port;
    int my_id;
    int store_size;
    int n_servers;
    int responses_needed;
    KeyValueStore store;
    MainLoop main_loop;
    Thread main_loop_worker;

    // linked to prioritize insertion order
    LinkedHashMap<Integer, PendingTransaction> pendingTransactions;
    PriorityQueue<PendingRequest> pendingRequests;

    // Paxos variables
    AtomicInteger currentIndex;
    private LinkedHashMap<Integer, PaxosInstance> instances;

    // Possible server configurations
    Integer[][] configs = {{0, 1, 2}, {1, 2, 3}, {2, 3, 4}};

    String[] targets;
    ManagedChannel[] channels;
    DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub[] async_stubs;

    public DadkvsServerState(int kv_size, int port, int myself) {
        base_port = port;
        my_id = myself;
        i_am_leader = false;
        is_freezed = false;
        slow_mode = false;
        store_size = kv_size;
        n_servers = 5;
        responses_needed = 2;
        store = new KeyValueStore(kv_size);
        main_loop = new MainLoop(this);

        pendingTransactions = new LinkedHashMap<>();
        pendingRequests = new PriorityQueue<>(Comparator.comparingInt(req -> req.getIndex()));

        currentIndex = new AtomicInteger(0);
        instances = new LinkedHashMap<>();

        targets = new String[n_servers];
        for (int i = 0; i < n_servers; i++) {
            targets[i] = new String();
            targets[i] = "localhost:" + (base_port + i);
        }

        main_loop_worker = new Thread(main_loop);
        main_loop_worker.start();
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

    public PaxosInstance getPaxos(int index) {
        if (this.instances.get(index) == null)
            this.instances.put(index, new PaxosInstance());
        return this.instances.get(index);
    }

}
