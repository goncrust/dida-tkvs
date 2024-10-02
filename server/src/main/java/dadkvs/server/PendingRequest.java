package dadkvs.server;

public class PendingRequest {
    private int reqid;
    private int index;

    public PendingRequest(int reqid, int index) {
        this.reqid = reqid;
        this.index = index;
    }

    // Getter methods for all fields
    public int getReqid() {
        return reqid;
    }

    public int getIndex() {
        return index;
    }
}
