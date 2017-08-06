package im.xiaoyao.presto.ethereum;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.DefaultBlockParameter;
import org.web3j.protocol.core.methods.response.EthBlock;

import java.io.IOException;
import java.math.BigInteger;
import java.util.concurrent.ConcurrentMap;

public class CachedWeb3j {
    private final Web3j web3j;
    private final DB db;
    private final ConcurrentMap cache;

    public CachedWeb3j(Web3j web3j) {
        this.web3j = web3j;
        this.db = DBMaker.memoryDB().make();
        this.cache = db.hashMap("web3j").createOrOpen();
    }

    public EthBlock.Block ethGetBlockByNumber(long blockNumber) throws IOException {
        if (cache.containsKey(blockNumber)) {
            return (EthBlock.Block) cache.get(blockNumber);
        }

        EthBlock.Block block = web3j.ethGetBlockByNumber(DefaultBlockParameter.valueOf(BigInteger.valueOf(blockNumber)), true)
                .send().getBlock();
        cache.put(blockNumber, block);
        return block;
    }
}
