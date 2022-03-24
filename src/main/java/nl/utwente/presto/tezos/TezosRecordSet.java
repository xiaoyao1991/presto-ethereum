package nl.utwente.presto.tezos;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.common.type.Type;
import com.google.common.collect.ImmutableList;
import nl.utwente.presto.tezos.handle.TezosColumnHandle;
import io.airlift.log.Logger;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.DefaultBlockParameter;
import org.web3j.protocol.core.methods.response.EthBlock;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class TezosRecordSet implements RecordSet {
    private static final Logger log = Logger.get(TezosRecordSet.class);

    private final TezosSplit split;
    private final Web3j web3j;

    private final List<TezosColumnHandle> columnHandles;
    private final List<Type> columnTypes;

    TezosRecordSet(Web3j web3j, List<TezosColumnHandle> columnHandles, TezosSplit split) {
        this.split = requireNonNull(split, "split is null");
        this.web3j = requireNonNull(web3j, "web3j is null");

        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");

        this.columnTypes = columnHandles.stream()
                .map(TezosColumnHandle::getType)
                .collect(Collectors.toList());
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
        return new TezosRecordCursor(columnHandles, block, split.getTable(), web3j);
    }
}
