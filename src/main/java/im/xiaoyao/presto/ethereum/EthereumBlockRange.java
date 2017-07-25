package im.xiaoyao.presto.ethereum;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class EthereumBlockRange {
    private final Long startBlock;
    private final Long endBlock;

    @JsonCreator
    public EthereumBlockRange(
            @JsonProperty("startBlock") Long startBlock,
            @JsonProperty("endBlock") Long endBlock
    ) {
        this.startBlock = startBlock;
        this.endBlock = endBlock;
    }

    @JsonProperty
    public Long getStartBlock() {
        return startBlock;
    }

    @JsonProperty
    public Long getEndBlock() {
        return endBlock;
    }
}
