package im.xiaoyao.presto.ethereum;

import io.airlift.configuration.Config;

public class EthereumConnectorConfig {
    public static final String DEFAULT_JSON_RPC = "http://localhost:8545/";
    private String ethereumJsonRpc;
    private String ethereumIpc;
    private String infuraRpc;

    @Config("ethereum.jsonrpc")
    public EthereumConnectorConfig setEthereumJsonRpc(String ethereumJsonRpc) {
        this.ethereumJsonRpc = ethereumJsonRpc;
        return this;
    }

    public String getEthereumJsonRpc() {
        return ethereumJsonRpc;
    }

    @Config("ethereum.ipc")
    public EthereumConnectorConfig setEthereumIpc(String ethereumIpc) {
        this.ethereumIpc = ethereumIpc;
        return this;
    }

    public String getEthereumIpc() {
        return ethereumIpc;
    }

    @Config("ethereum.infura")
    public EthereumConnectorConfig setInfuraRpc(String infuraRpc) {
        this.infuraRpc = infuraRpc;
        return this;
    }

    public String getInfuraRpc() {
        return infuraRpc;
    }
}
