package im.xiaoyao.presto.ethereum;

import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.ToString;

import static java.util.Objects.requireNonNull;

@ToString
public class EthereumTableLayoutHandle implements ConnectorTableLayoutHandle {
    private final EthereumTableHandle table;

    private final long startBlock;
    private final long endBlock;

    @JsonCreator
    public EthereumTableLayoutHandle(
            @JsonProperty("table") EthereumTableHandle table,
            @JsonProperty("startBlock") Long startBlock,
            @JsonProperty("endBlock") Long endBlock
    ) {
        this.table = requireNonNull(table, "table is null");
        this.startBlock = startBlock == null ? 1 : startBlock;
        this.endBlock = endBlock == null ? -1 : endBlock;
    }

    @JsonProperty
    public EthereumTableHandle getTable() {
        return table;
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
