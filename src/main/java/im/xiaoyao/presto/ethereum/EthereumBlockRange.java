package im.xiaoyao.presto.ethereum;

import com.facebook.presto.spi.predicate.Marker;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import sun.jvm.hotspot.oops.Mark;

import java.util.Iterator;

public class EthereumBlockRange implements Iterator<Long> {
    private final long startBlock;
    private final long endBlock;
    private long cursor;

    public EthereumBlockRange(Marker low, Marker high) {
        if (low.isLowerUnbounded()) {
            this.startBlock = 1L;
        } else if (low.getBound() == Marker.Bound.EXACTLY) {
            this.startBlock = (long) low.getValue();
        } else if (low.getBound() == Marker.Bound.ABOVE) {
            this.startBlock = (long) low.getValue() + 1L;
        } else {
            throw new IllegalArgumentException("Low bound cannot be BELOW");
        }

        if (high.isUpperUnbounded()) {
            this.endBlock = -1L;
        } else if (high.getBound() == Marker.Bound.EXACTLY) {
            this.endBlock = (long) high.getValue();
        } else if (high.getBound() == Marker.Bound.BELOW) {
            this.endBlock = (long) high.getValue() - 1L;
        } else {
            throw new IllegalArgumentException("High bound cannot be ABOVE");
        }

        if (startBlock > endBlock) {
            throw new IllegalArgumentException("Low bound is greater than high bound");
        }

        this.cursor = this.startBlock;
    }

    public EthereumBlockRange(long blockNumber) {
        this.startBlock = blockNumber;
        this.endBlock = blockNumber;
        this.cursor = this.startBlock;
    }

    public long getStartBlock() {
        return startBlock;
    }

    public long getEndBlock() {
        return endBlock;
    }

    @Override
    public boolean hasNext() {
        return cursor <= endBlock;
    }

    @Override
    public Long next() {
        return cursor++;
    }
}
