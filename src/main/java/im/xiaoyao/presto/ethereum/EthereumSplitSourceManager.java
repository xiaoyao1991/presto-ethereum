package im.xiaoyao.presto.ethereum;

import com.facebook.presto.spi.ConnectorSplit;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class EthereumSplitSourceManager {
    private final ConcurrentHashMap<String, List<EthereumSplitSource>> splitSources;

    @Inject
    public EthereumSplitSourceManager() {
        this.splitSources = new ConcurrentHashMap<>();
    }

//    public EthereumSplitSource get(String queryId) {
//        return splitSources.get(queryId);
//    }

    // Inline change
    public EthereumSplitSource put(String queryId, List<ConnectorSplit> splits) {
        EthereumSplitSource newSplitSource = new EthereumSplitSource(queryId, splits);
        if (!splitSources.containsKey(queryId)) {
            List<EthereumSplitSource> lst = new ArrayList<>();
            lst.add(newSplitSource);
            this.splitSources.put(queryId, lst);
        } else {
            List<EthereumSplitSource> lst = this.splitSources.get(queryId);
            for (EthereumSplitSource splitSource : lst) {
                splitSource.updateSplits(splits);
            }
            lst.add(newSplitSource);
        }
        return newSplitSource;
    }

    public void removeSource(String queryId) {
        splitSources.remove(queryId);
    }
}
