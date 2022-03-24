package nl.utwente.presto.tezos.handle;

import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import nl.utwente.presto.tezos.TezosBlockRange;
import lombok.ToString;

import java.util.List;

import static java.util.Objects.requireNonNull;

@ToString
public class TezosTableLayoutHandle implements ConnectorTableLayoutHandle {
    private final TezosTableHandle table;

    private final List<TezosBlockRange> blockRanges;

    @JsonCreator
    public TezosTableLayoutHandle(
            @JsonProperty("table") TezosTableHandle table,
            @JsonProperty("blockRanges") List<TezosBlockRange> blockRanges
    ) {
        this.table = requireNonNull(table, "table is null");
        this.blockRanges = requireNonNull(blockRanges, "blockRanges is null");
    }

    @JsonProperty
    public TezosTableHandle getTable() {
        return table;
    }

    @JsonProperty
    public List<TezosBlockRange> getBlockRanges() {
        return blockRanges;
    }
}
