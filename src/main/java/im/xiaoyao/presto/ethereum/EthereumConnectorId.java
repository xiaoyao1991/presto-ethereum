package im.xiaoyao.presto.ethereum;

import lombok.EqualsAndHashCode;
import lombok.ToString;

import static java.util.Objects.requireNonNull;

@EqualsAndHashCode
@ToString
public class EthereumConnectorId {
    private final String connectorId;

    public EthereumConnectorId(String connectorId) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
    }
}
