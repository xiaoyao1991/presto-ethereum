package nl.utwente.presto.tezos;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import nl.utwente.presto.tezos.handle.TezosColumnHandle;
import nl.utwente.presto.tezos.handle.TezosHandleResolver;
import org.web3j.protocol.Web3j;

import javax.inject.Inject;
import java.util.List;
import java.util.stream.Collectors;

import static nl.utwente.presto.tezos.handle.TezosHandleResolver.convertSplit;

public class TezosRecordSetProvider implements ConnectorRecordSetProvider {
    private final Web3j web3j;

    @Inject
    public TezosRecordSetProvider(TezosWeb3jProvider web3jProvider) {
        this.web3j = web3jProvider.getWeb3j();
    }

    @Override
    public RecordSet getRecordSet(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            List<? extends ColumnHandle> columns
    ) {
        TezosSplit tezosSplit = convertSplit(split);

        List<TezosColumnHandle> columnHandles = columns.stream()
                .map(TezosHandleResolver::convertColumnHandle)
                .collect(Collectors.toList());

        return new TezosRecordSet(web3j, columnHandles, tezosSplit);
    }
}
