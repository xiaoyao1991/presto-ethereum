package im.xiaoyao.presto.ethereum;

import io.airlift.configuration.Config;

public class EthereumConnectorConfig {
    private static final int ETHEREUM_DEFAULT_PORT = 9092;

    private String ethereumNodeEndpoint = "";

    @Config("ethereum.node-endpoint")
    public EthereumConnectorConfig setEthereumNodeEndpoint(String ethereumNodeEndpoint) {
        this.ethereumNodeEndpoint = ethereumNodeEndpoint;
        return this;
    }

    public String getEthereumNodeEndpoint() {
        return ethereumNodeEndpoint;
    }
}
