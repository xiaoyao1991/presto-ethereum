package nl.utwente.presto.tezos;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeProvider;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;

public class TezosSplit implements ConnectorSplit {
    private final long blockId;
    private final String blockHash;

    private final TezosTable table;

    @JsonCreator
    public TezosSplit(
            @JsonProperty("blockId") long blockId,
            @JsonProperty("table") TezosTable table
    ) {
        this.blockId = blockId;
        this.table = table;
        this.blockHash = null;
    }

    @JsonProperty
    public long getBlockId() {
        return blockId;
    }

    @JsonProperty
    public String getBlockHash() {
        return blockHash;
    }

    @JsonProperty
    public TezosTable getTable() {
        return table;
    }

    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy() {
        return NodeSelectionStrategy.NO_PREFERENCE;
    }

    @Override
    public List<HostAddress> getPreferredNodes(NodeProvider nodeProvider) {
        return Collections.emptyList();
    }

    @Override
    public Object getInfo() {
        return this;
    }
}
