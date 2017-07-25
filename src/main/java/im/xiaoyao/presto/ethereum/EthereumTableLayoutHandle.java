package im.xiaoyao.presto.ethereum;

import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.ToString;

import java.util.List;

import static java.util.Objects.requireNonNull;

@ToString
public class EthereumTableLayoutHandle implements ConnectorTableLayoutHandle {
    private final EthereumTableHandle table;

    private final List<EthereumBlockRange> blockRanges;

    @JsonCreator
    public EthereumTableLayoutHandle(
            @JsonProperty("table") EthereumTableHandle table,
            @JsonProperty("blockRanges") List<EthereumBlockRange> blockRanges
    ) {
        this.table = requireNonNull(table, "table is null");
        this.blockRanges = requireNonNull(blockRanges, "blockRanges is null");
    }

    @JsonProperty
    public EthereumTableHandle getTable() {
        return table;
    }

    @JsonProperty
    public List<EthereumBlockRange> getBlockRanges() {
        return blockRanges;
    }
}
