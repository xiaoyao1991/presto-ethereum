package im.xiaoyao.presto.tezos.connector;

import io.airlift.configuration.Config;

public class TezosConnectorConfig {
    public static final String DEFAULT_JSON_RPC = "http://localhost:8545/";
    private String tezosJsonRpc;
    private String tezosIpc;
    private String infuraRpc;

    @Config("tezos.jsonrpc")
    public TezosConnectorConfig setTezosJsonRpc(String tezosJsonRpc) {
        this.tezosJsonRpc = tezosJsonRpc;
        return this;
    }

    public String getTezosJsonRpc() {
        return tezosJsonRpc;
    }

    @Config("tezos.ipc")
    public TezosConnectorConfig setTezosIpc(String tezosIpc) {
        this.tezosIpc = tezosIpc;
        return this;
    }

    public String getTezosIpc() {
        return tezosIpc;
    }

    @Config("tezos.infura")
    public TezosConnectorConfig setInfuraRpc(String infuraRpc) {
        this.infuraRpc = infuraRpc;
        return this;
    }

    public String getInfuraRpc() {
        return infuraRpc;
    }
}
