package im.xiaoyao.presto.ethereum.handle;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import im.xiaoyao.presto.ethereum.EthereumSplit;
import im.xiaoyao.presto.ethereum.handle.EthereumColumnHandle;
import im.xiaoyao.presto.ethereum.handle.EthereumTableHandle;
import im.xiaoyao.presto.ethereum.handle.EthereumTableLayoutHandle;
import im.xiaoyao.presto.ethereum.handle.EthereumTransactionHandle;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class EthereumHandleResolver implements ConnectorHandleResolver {
    @Override
    public Class<? extends ConnectorTableHandle> getTableHandleClass() {
        return EthereumTableHandle.class;
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass() {
        return EthereumColumnHandle.class;
    }

    @Override
    public Class<? extends ConnectorSplit> getSplitClass() {
        return EthereumSplit.class;
    }

    @Override
    public Class<? extends ConnectorTableLayoutHandle> getTableLayoutHandleClass() {
        return EthereumTableLayoutHandle.class;
    }

    @Override
    public Class<? extends ConnectorTransactionHandle> getTransactionHandleClass() {
        return EthereumTransactionHandle.class;
    }

    public static EthereumTableHandle convertTableHandle(ConnectorTableHandle tableHandle) {
        requireNonNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof EthereumTableHandle, "tableHandle is not an instance of EthereumTableHandle");
        return (EthereumTableHandle) tableHandle;
    }

    public static EthereumColumnHandle convertColumnHandle(ColumnHandle columnHandle) {
        requireNonNull(columnHandle, "columnHandle is null");
        checkArgument(columnHandle instanceof EthereumColumnHandle, "columnHandle is not an instance of EthereumColumnHandle");
        return (EthereumColumnHandle) columnHandle;
    }

    public static EthereumSplit convertSplit(ConnectorSplit split) {
        requireNonNull(split, "split is null");
        checkArgument(split instanceof EthereumSplit, "split is not an instance of EthereumSplit");
        return (EthereumSplit) split;
    }

    public static EthereumTableLayoutHandle convertLayout(ConnectorTableLayoutHandle layout) {
        requireNonNull(layout, "layout is null");
        checkArgument(layout instanceof EthereumTableLayoutHandle, "layout is not an instance of EthereumTableLayoutHandle");
        return (EthereumTableLayoutHandle) layout;
    }
}
