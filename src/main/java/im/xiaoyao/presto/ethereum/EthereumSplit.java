package im.xiaoyao.presto.ethereum;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;

public class EthereumSplit implements ConnectorSplit {
    private final long blockId;
    private final String blockHash;

    private final EthereumTable table;

    @JsonCreator
    public EthereumSplit(
            @JsonProperty("blockId") long blockId,
            @JsonProperty("table") EthereumTable table
    ) {
        this.blockId = blockId;
        this.table = table;
        this.blockHash = null;
    }

//    @JsonCreator
//    public EthereumSplit(@JsonProperty("blockHash") String blockHash) {
//        this.blockId = -1L;
//        this.blockHash = blockHash;
//    }

    @JsonProperty
    public long getBlockId() {
        return blockId;
    }

    @JsonProperty
    public String getBlockHash() {
        return blockHash;
    }

    @JsonProperty
    public EthereumTable getTable() {
        return table;
    }

    @Override
    public boolean isRemotelyAccessible() {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses() {
        return Collections.emptyList();
    }

    @Override
    public Object getInfo() {
        return this;
    }
}
