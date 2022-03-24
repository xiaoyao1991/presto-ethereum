package im.xiaoyao.presto.tezos;

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import im.xiaoyao.presto.tezos.connector.TezosConnectorFactory;
import im.xiaoyao.presto.tezos.udfs.TezosUDFs;

import java.util.Set;

public class TezosPlugin implements Plugin {
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories() {
        return ImmutableList.of(new TezosConnectorFactory());
    }

    @Override
    public Set<Class<?>> getFunctions() {
        return ImmutableSet.of(TezosUDFs.class);
    }
}
