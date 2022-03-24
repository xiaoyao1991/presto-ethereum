package nl.utwente.presto.tezos.udfs;

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.common.type.StandardTypes;
import nl.utwente.presto.tezos.connector.TezosConnectorConfig;
import io.airlift.configuration.ConfigurationLoader;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.DefaultBlockParameter;
import org.web3j.protocol.http.HttpService;
import org.web3j.protocol.infura.InfuraHttpService;
import org.web3j.protocol.ipc.UnixIpcService;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Map;

public class TezosUDFs {
    private static final Logger log = Logger.get(TezosUDFs.class);
    private static final String CONFIG_PATH = "etc/catalog/tezos.properties";
    private static final String JSONRPC_KEY = "tezos.jsonrpc";
    private static final String IPC_KEY = "tezos.ipc";
    private static final String INFURA_KEY = "tezos.infura";
    private static final String LATEST = "latest";
    private static final String ZERO_X = "0x";
    private static final Web3j web3j;

    // A hack, which I don't like
    static {
        log.info("Initializing Web3j in UDF...");
        ConfigurationLoader configLoader = new ConfigurationLoader();
        try {
            Map<String, String> config = configLoader.loadPropertiesFrom(CONFIG_PATH);
            if (config.get(JSONRPC_KEY) == null
                    && config.get(IPC_KEY) == null
                    && config.get(INFURA_KEY) == null) {
                web3j = Web3j.build(new HttpService(TezosConnectorConfig.DEFAULT_JSON_RPC));
            } else if (config.get(JSONRPC_KEY) != null
                    && config.get(IPC_KEY) == null
                    && config.get(INFURA_KEY) == null) {
                web3j = Web3j.build(new HttpService(config.get(JSONRPC_KEY)));
            } else if (config.get(JSONRPC_KEY) == null
                    && config.get(IPC_KEY) != null
                    && config.get(INFURA_KEY) == null) {
                web3j = Web3j.build(new UnixIpcService(config.get(IPC_KEY)));
            } else if (config.get(JSONRPC_KEY) == null
                    && config.get(IPC_KEY) == null
                    && config.get(INFURA_KEY) != null) {
                web3j = Web3j.build(new InfuraHttpService(config.get(INFURA_KEY)));
            } else {
                throw new IllegalArgumentException("More than 1 Tezos service providers found");
            }
        } catch (IOException e) {
            throw new IllegalStateException("Cannot load config from " + CONFIG_PATH);
        }
    }

    @ScalarFunction("eth_gasPrice")
    @Description("Returns current gas price")
    @SqlType(StandardTypes.DOUBLE)
    public static double ethGasPrice() throws IOException {
        return web3j.ethGasPrice().send().getGasPrice().doubleValue();
    }

    @ScalarFunction("eth_blockNumber")
    @Description("Returns current block number")
    @SqlType(StandardTypes.BIGINT)
    public static long ethBlockNumber() throws IOException {
        return web3j.ethBlockNumber().send().getBlockNumber().longValue();
    }

    @ScalarFunction("eth_getBalance")
    @Description("Returns the balance of an address")
    @SqlType(StandardTypes.DOUBLE)
    public static double ethGetBalance(@SqlType(StandardTypes.VARCHAR) Slice address) throws IOException {
        return web3j.ethGetBalance(address.toStringUtf8(), DefaultBlockParameter.valueOf(LATEST)).send().getBalance().doubleValue();
    }

    @ScalarFunction("eth_getBalance")
    @Description("Returns the balance of an address")
    @SqlType(StandardTypes.DOUBLE)
    public static double ethGetBalance(@SqlType(StandardTypes.VARCHAR) Slice address, @SqlType(StandardTypes.BIGINT) long blockNumber) throws IOException {
        return web3j.ethGetBalance(address.toStringUtf8(), DefaultBlockParameter.valueOf(BigInteger.valueOf(blockNumber))).send().getBalance().doubleValue();
    }

    @ScalarFunction("eth_getBalance")
    @Description("Returns the balance of an address")
    @SqlType(StandardTypes.DOUBLE)
    public static double ethGetBalance(@SqlType(StandardTypes.VARCHAR) Slice address, @SqlType(StandardTypes.VARCHAR) Slice blockName) throws IOException {
        return web3j.ethGetBalance(address.toStringUtf8(), DefaultBlockParameter.valueOf(blockName.toStringUtf8())).send().getBalance().doubleValue();
    }

    @ScalarFunction("eth_getTransactionCount")
    @Description("Returns the number of transactions from this address")
    @SqlType(StandardTypes.BIGINT)
    public static long ethGetTransactionCount(@SqlType(StandardTypes.VARCHAR) Slice address) throws IOException {
        return web3j.ethGetTransactionCount(address.toStringUtf8(), DefaultBlockParameter.valueOf(LATEST)).send().getTransactionCount().longValue();
    }

    @ScalarFunction("eth_getTransactionCount")
    @Description("Returns the number of transactions from this address")
    @SqlType(StandardTypes.BIGINT)
    public static long ethGetTransactionCount(@SqlType(StandardTypes.VARCHAR) Slice address, @SqlType(StandardTypes.BIGINT) long blockNumber) throws IOException {
        return web3j.ethGetTransactionCount(address.toStringUtf8(), DefaultBlockParameter.valueOf(BigInteger.valueOf(blockNumber))).send().getTransactionCount().longValue();
    }

    @ScalarFunction("eth_getTransactionCount")
    @Description("Returns the number of transactions from this address")
    @SqlType(StandardTypes.BIGINT)
    public static long ethGetTransactionCount(@SqlType(StandardTypes.VARCHAR) Slice address, @SqlType(StandardTypes.VARCHAR) Slice blockName) throws IOException {
        return web3j.ethGetTransactionCount(address.toStringUtf8(), DefaultBlockParameter.valueOf(blockName.toStringUtf8())).send().getTransactionCount().longValue();
    }

    @ScalarFunction("fromWei")
    @Description("fromWei")
    @SqlType(StandardTypes.DOUBLE)
    public static double fromWei(@SqlType(StandardTypes.DOUBLE) double num, @SqlType(StandardTypes.VARCHAR) Slice unit) {
        String unitStr = unit.toStringUtf8().toUpperCase();
        TezosUnit u = TezosUnit.valueOf(unitStr);
        return u.fromWei(num);
    }

    @ScalarFunction("toWei")
    @Description("toWei")
    @SqlType(StandardTypes.DOUBLE)
    public static double toWei(@SqlType(StandardTypes.DOUBLE) double num, @SqlType(StandardTypes.VARCHAR) Slice unit) {
        String unitStr = unit.toStringUtf8().toUpperCase();
        TezosUnit u = TezosUnit.valueOf(unitStr);
        return u.toWei(num);
    }

    @ScalarFunction("isContract")
    @Description("isContract")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isContract(@SqlType(StandardTypes.VARCHAR) Slice address) throws IOException {
        return !web3j.ethGetCode(address.toStringUtf8(), DefaultBlockParameter.valueOf(LATEST)).send().getCode().equals(ZERO_X);
    }
}
