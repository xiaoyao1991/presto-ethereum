package im.xiaoyao.presto.ethereum;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import io.airlift.log.Logger;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.concurrent.CompletableFuture.completedFuture;

public class EthereumSplitSource implements ConnectorSplitSource {
    private static final Logger log = Logger.get(EthereumSplitSource.class);

    private final String queryId;
    private AtomicBoolean readOnly;
    private CopyOnWriteArrayList<ConnectorSplit> splits;

    private AtomicInteger offset;

    public EthereumSplitSource(String queryId, List<ConnectorSplit> splits) {
        this.queryId = queryId;
        this.readOnly = new AtomicBoolean(false);
        this.splits = new CopyOnWriteArrayList<>(splits);
        this.offset = new AtomicInteger();
    }

    @Override
    public synchronized CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize) {
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
    public synchronized boolean isFinished() {
        return offset.get() >= splits.size();
    }

    public synchronized boolean updateSplits(List<ConnectorSplit> splits) {
        if (readOnly.get()) {
            log.error("Cannot replace a closed split source");
            return false;
        }

        log.info("removing elements...");

        // ????
        EthereumTable table = ((EthereumSplit) this.splits.get(0)).getTable();
        List<EthereumSplit> newSplits = splits.stream()
                .map(s -> new EthereumSplit(((EthereumSplit) s).getBlockId(), table))
                .collect(Collectors.toList());

        this.splits = new CopyOnWriteArrayList<>(newSplits);
        log.info("finished removing elements...");

        this.offset.set(0);

        log.info("Replacing splits for query %s", queryId);
        return true;
    }
}
