package im.xiaoyao.presto.ethereum.connector;

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.inject.Injector;
import im.xiaoyao.presto.ethereum.handle.EthereumHandleResolver;
import io.airlift.bootstrap.Bootstrap;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class EthereumConnectorFactory implements ConnectorFactory {
    @Override
    public String getName() {
        return "ethereum";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver() {
        return new EthereumHandleResolver();
    }

    @Override
    public Connector create(String connectorId, Map<String, String> config, ConnectorContext context) {
        requireNonNull(config, "config is null");

        try {
            Bootstrap app = new Bootstrap(
                    new EthereumConnectorModule()
            );

            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            return injector.getInstance(EthereumConnector.class);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
