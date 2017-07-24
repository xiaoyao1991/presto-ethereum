package im.xiaoyao.presto.ethereum;

import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.ToString;

import static java.util.Objects.requireNonNull;

@ToString
public class EthereumTableLayoutHandle implements ConnectorTableLayoutHandle {
    private final EthereumTableHandle table;

    @JsonCreator
    public EthereumTableLayoutHandle(@JsonProperty("table") EthereumTableHandle table) {
        this.table = requireNonNull(table, "table is null");
    }

    @JsonProperty
    public EthereumTableHandle getTable() {
        return table;
    }
}
