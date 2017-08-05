package im.xiaoyao.presto.ethereum;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import io.airlift.log.Logger;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.CompletableFuture.completedFuture;

@ThreadSafe
public class EthereumSplitSource implements ConnectorSplitSource {
    private static final Logger log = Logger.get(EthereumSplitSource.class);

    private final String queryId;
    private AtomicBoolean readOnly;
    private volatile CopyOnWriteArrayList<ConnectorSplit> splits;

    private AtomicInteger offset;

    public EthereumSplitSource(AtomicBoolean readOnly, List<ConnectorSplit> splits, String queryId) {
        this.queryId = queryId;
        this.readOnly = readOnly;
        this.splits = new CopyOnWriteArrayList<>(splits);
        this.offset = new AtomicInteger();
    }

    @Override
    public CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize) {
        int offset = this.offset.get();
        int remainingSplits = splits.size() - offset;
        int size = Math.min(remainingSplits, maxSize);
        List<ConnectorSplit> results = splits.subList(offset, offset + size);
        this.offset.set(offset + size);

        return completedFuture(results);
    }

    @Override
    public void close() {
        readOnly.set(true);
    }

    @Override
    public boolean isFinished() {
        return offset.get() >= splits.size();
    }

    public synchronized boolean updateSplits(List<ConnectorSplit> splits) {
        if (readOnly.get()) {
            log.error("Cannot replace a closed split source");
            return false;
        }

        this.splits = new CopyOnWriteArrayList<>(splits);
        this.offset = new AtomicInteger();
        log.info("Replacing splits for query %s", queryId);
        return true;
    }
}
