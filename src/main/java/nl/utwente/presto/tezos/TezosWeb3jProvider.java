package im.xiaoyao.presto.tezos;

import im.xiaoyao.presto.tezos.connector.TezosConnectorConfig;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.http.HttpService;
import org.web3j.protocol.infura.InfuraHttpService;
import org.web3j.protocol.ipc.UnixIpcService;

import javax.inject.Inject;

public class TezosWeb3jProvider {
    private final Web3j web3j;

    @Inject
    public TezosWeb3jProvider(TezosConnectorConfig config) {
        if (config.getTezosJsonRpc() == null
                && config.getTezosIpc() == null
                && config.getInfuraRpc() == null) {
            this.web3j = Web3j.build(new HttpService(TezosConnectorConfig.DEFAULT_JSON_RPC));
        } else if (config.getTezosJsonRpc() != null
                && config.getTezosIpc() == null
                && config.getInfuraRpc() == null) {
            this.web3j = Web3j.build(new HttpService(config.getTezosJsonRpc()));
        } else if (config.getTezosJsonRpc() == null
                && config.getTezosIpc() != null
                && config.getInfuraRpc() == null) {
            this.web3j = Web3j.build(new UnixIpcService(config.getTezosIpc()));
        } else if (config.getTezosJsonRpc() == null
                && config.getTezosIpc() == null
                && config.getInfuraRpc() != null) {
            this.web3j = Web3j.build(new InfuraHttpService(config.getInfuraRpc()));
        } else {
            throw new IllegalArgumentException("More than 1 Tezos service providers found");
        }
    }

    public Web3j getWeb3j() {
        return web3j;
    }
}
