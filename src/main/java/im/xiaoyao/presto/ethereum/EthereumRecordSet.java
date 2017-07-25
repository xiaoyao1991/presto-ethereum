package im.xiaoyao.presto.ethereum;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.DefaultBlockParameter;
import org.web3j.protocol.core.methods.response.EthBlock;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class EthereumRecordSet implements RecordSet {
    private static final Logger log = Logger.get(EthereumRecordSet.class);

    private final EthereumSplit split;
    private final Web3j web3j;   //TODO: where to put web3j, or have a web3j manager

    private final List<EthereumColumnHandle> columnHandles;
    private final List<Type> columnTypes;

    EthereumRecordSet(Web3j web3j, List<EthereumColumnHandle> columnHandles, EthereumSplit split) {
        this.split = requireNonNull(split, "split is null");
        this.web3j = requireNonNull(web3j, "web3j is null");

        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");

        ImmutableList.Builder<Type> typeBuilder = ImmutableList.builder();

        for (EthereumColumnHandle handle : columnHandles) {
            typeBuilder.add(handle.getType());
        }

        this.columnTypes = typeBuilder.build();
    }

    @Override
    public List<Type> getColumnTypes() {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor() {
        EthBlock block = null;
        try {
            block = web3j.ethGetBlockByNumber(DefaultBlockParameter.valueOf(BigInteger.valueOf(split.getBlockId())), true).send();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new EthereumRecordCursor(columnHandles, block, split.getTable());
    }
}
