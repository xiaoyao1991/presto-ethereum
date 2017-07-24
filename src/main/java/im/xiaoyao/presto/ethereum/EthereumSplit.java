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

    @JsonCreator
    public EthereumSplit(@JsonProperty("blockId") long blockId) {
        this.blockId = blockId;
        this.blockHash = null;
    }

    @JsonCreator
    public EthereumSplit(@JsonProperty("blockHash") String blockHash) {
        this.blockId = -1L;
        this.blockHash = blockHash;
    }

    @JsonProperty
    public long getBlockId() {
        return blockId;
    }

    @JsonProperty
    public String getBlockHash() {
        return blockHash;
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
