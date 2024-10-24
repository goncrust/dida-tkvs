package dadkvs.server;

import dadkvs.DadkvsConsole;
import dadkvs.DadkvsConsoleServiceGrpc;
import io.grpc.stub.StreamObserver;

public class DadkvsConsoleServiceImpl
        extends DadkvsConsoleServiceGrpc.DadkvsConsoleServiceImplBase {

    DadkvsServerState server_state;

    public DadkvsConsoleServiceImpl(DadkvsServerState state) {
        this.server_state = state;
    }

    @Override
    public void setleader(DadkvsConsole.SetLeaderRequest request,
            StreamObserver<DadkvsConsole.SetLeaderReply> responseObserver) {
        // for debug purposes
        System.out.println(request);

        boolean response_value = true;
        this.server_state.i_am_leader = request.getIsleader();

        // for debug purposes
        System.out.println("I am the leader = " + this.server_state.i_am_leader);

        this.server_state.main_loop.wakeup();

        DadkvsConsole.SetLeaderReply response =
                DadkvsConsole.SetLeaderReply.newBuilder().setIsleaderack(response_value).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void setdebug(DadkvsConsole.SetDebugRequest request,
            StreamObserver<DadkvsConsole.SetDebugReply> responseObserver) {
        // for debug purposes
        System.out.println(request);

        boolean response_value = true;
        int mode = request.getMode();

        if (mode == 1) {
            System.exit(1);
        } else if (mode == 2) {
            this.server_state.is_freezed = true;
        } else if (mode == 3) {
            this.server_state.is_freezed = false;
        } else if (mode == 4) {
            this.server_state.slow_mode = true;
        } else if (mode == 5) {
            this.server_state.slow_mode = false;
        }

        this.server_state.main_loop.wakeup();

        // for debug purposes
        System.out.println("Setting debug mode to = " + mode);

        DadkvsConsole.SetDebugReply response =
                DadkvsConsole.SetDebugReply.newBuilder().setAck(response_value).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
