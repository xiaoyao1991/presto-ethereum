package im.xiaoyao.presto.ethereum;

import im.xiaoyao.presto.ethereum.connector.EthereumConnectorConfig;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.http.HttpService;
import org.web3j.protocol.infura.InfuraHttpService;
import org.web3j.protocol.ipc.UnixIpcService;

import javax.inject.Inject;

public class EthereumWeb3jProvider {
    private final Web3j web3j;

    @Inject
    public EthereumWeb3jProvider(EthereumConnectorConfig config) {
        if (config.getEthereumJsonRpc() == null
                && config.getEthereumIpc() == null
                && config.getInfuraRpc() == null) {
            this.web3j = Web3j.build(new HttpService(EthereumConnectorConfig.DEFAULT_JSON_RPC));
        } else if (config.getEthereumJsonRpc() != null
                && config.getEthereumIpc() == null
                && config.getInfuraRpc() == null) {
            this.web3j = Web3j.build(new HttpService(config.getEthereumJsonRpc()));
        } else if (config.getEthereumJsonRpc() == null
                && config.getEthereumIpc() != null
                && config.getInfuraRpc() == null) {
            this.web3j = Web3j.build(new UnixIpcService(config.getEthereumIpc()));
        } else if (config.getEthereumJsonRpc() == null
                && config.getEthereumIpc() == null
                && config.getInfuraRpc() != null) {
            this.web3j = Web3j.build(new InfuraHttpService(config.getInfuraRpc()));
        } else {
            throw new IllegalArgumentException("More than 1 Ethereum service providers found");
        }
    }

    public Web3j getWeb3j() {
        return web3j;
    }
}
