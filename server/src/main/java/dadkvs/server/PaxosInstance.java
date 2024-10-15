package dadkvs.server;

import java.util.concurrent.atomic.AtomicInteger;

public class PaxosInstance {
    AtomicInteger rnd;
    AtomicInteger vrnd;
    AtomicInteger vval;

    public PaxosInstance() {
        this.rnd = new AtomicInteger(-1);
        this.vrnd = new AtomicInteger(0);
        this.vval = new AtomicInteger(-1);
    }

    public int getRnd() {
        return rnd.get();
    }

    public void setRnd(int rnd) {
        this.rnd.set(rnd);
    }

    public int getVrnd() {
        return vrnd.get();
    }

    public void setVrnd(int vrnd) {
        this.vrnd.set(vrnd);
    }

    public int getVval() {
        return vval.get();
    }

    public void setVval(int vval) {
        this.vval.set(vval);
    }
}
