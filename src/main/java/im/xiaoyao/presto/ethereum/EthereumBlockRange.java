package im.xiaoyao.presto.ethereum;

import com.facebook.presto.spi.predicate.Marker;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class EthereumBlockRange {
    private final long startBlock;
    private final long endBlock;

    public static EthereumBlockRange fromMarkers(Marker low, Marker high) {
        long startBlock;
        long endBlock;
        if (low.isLowerUnbounded()) {
            startBlock = 1L;
        } else if (low.getBound() == Marker.Bound.EXACTLY) {
            startBlock = (long) low.getValue();
        } else if (low.getBound() == Marker.Bound.ABOVE) {
            startBlock = (long) low.getValue() + 1L;
        } else {
            throw new IllegalArgumentException("Low bound cannot be BELOW");
        }

        if (high.isUpperUnbounded()) {
            endBlock = -1L;
        } else if (high.getBound() == Marker.Bound.EXACTLY) {
            endBlock = (long) high.getValue();
        } else if (high.getBound() == Marker.Bound.BELOW) {
            endBlock = (long) high.getValue() - 1L;
        } else {
            throw new IllegalArgumentException("High bound cannot be ABOVE");
        }

        if (startBlock > endBlock && endBlock != -1L) {
            throw new IllegalArgumentException("Low bound is greater than high bound");
        }

        return new EthereumBlockRange(startBlock, endBlock);
    }

    @JsonCreator
    public EthereumBlockRange(
            @JsonProperty("startBlock") long startBlock,
            @JsonProperty("endBlock") long endBlock
    ) {
        this.startBlock = startBlock;
        this.endBlock = endBlock;
    }

    @JsonProperty
    public long getStartBlock() {
        return startBlock;
    }

    @JsonProperty
    public long getEndBlock() {
        return endBlock;
    }
}
