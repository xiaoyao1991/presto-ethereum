package im.xiaoyao.presto.ethereum;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import im.xiaoyao.presto.ethereum.handle.EthereumColumnHandle;
import im.xiaoyao.presto.ethereum.handle.EthereumHandleResolver;
import org.web3j.protocol.Web3j;

import javax.inject.Inject;
import java.util.List;
import java.util.stream.Collectors;

import static im.xiaoyao.presto.ethereum.handle.EthereumHandleResolver.convertSplit;

public class EthereumRecordSetProvider implements ConnectorRecordSetProvider {
    private final Web3j web3j;

    @Inject
    public EthereumRecordSetProvider(EthereumWeb3jProvider web3jProvider) {
        this.web3j = web3jProvider.getWeb3j();
    }

    @Override
    public RecordSet getRecordSet(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            List<? extends ColumnHandle> columns
    ) {
        EthereumSplit ethereumSplit = convertSplit(split);

        List<EthereumColumnHandle> columnHandles = columns.stream()
                .map(EthereumHandleResolver::convertColumnHandle)
                .collect(Collectors.toList());

        return new EthereumRecordSet(web3j, columnHandles, ethereumSplit);
    }
}
