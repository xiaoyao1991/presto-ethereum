package im.xiaoyao.presto.tezos;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.collect.ImmutableList;
import im.xiaoyao.presto.tezos.handle.TezosColumnHandle;
import im.xiaoyao.presto.tezos.handle.TezosTableHandle;
import io.airlift.log.Logger;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static im.xiaoyao.presto.tezos.handle.TezosHandleResolver.convertColumnHandle;
import static im.xiaoyao.presto.tezos.handle.TezosHandleResolver.convertTableHandle;
import static java.util.Objects.requireNonNull;

public abstract class BaseTezosMetadata implements ConnectorMetadata {
    protected static final Logger log = Logger.get(TezosMetadata.class);

    protected static final String DEFAULT_SCHEMA = "default";

    /**
     * Get schemas
     * @param session
     * @return available schemas
     */
    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return Collections.singletonList(DEFAULT_SCHEMA);
    }

    /**
     * Get handle for table
     * @param session
     * @param schemaTableName table name
     * @return table handle
     */
    @Override
    public TezosTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName) {
        if (Arrays.stream(TezosTable.values())
                .noneMatch(table -> table.getName().equals(schemaTableName.getTableName()))) {
            throw new IllegalArgumentException("Unknown Table Name " + schemaTableName.getTableName());
        }

        return new TezosTableHandle(DEFAULT_SCHEMA, schemaTableName.getTableName());
    }

    /**
     * Returns all tables
     * @param session
     * @param schemaName schema to return the tables of
     * @return list of tables
     */
    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
        return Arrays.stream(TezosTable.values())
                .map(table -> new SchemaTableName(DEFAULT_SCHEMA, table.getName()))
                .collect(Collectors.toList());
    }


    /**
     * Returns all tables
     * @param session
     * @param schemaNameOrNull schema to return the tables of
     * @return list of tables
     */
    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        return listTables(session, Optional.ofNullable(schemaNameOrNull));
    }

    /**
     * Return the columns of tables
     * @param session
     * @param prefix tables
     * @return tables with columns
     */
    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        List<SchemaTableName> tableNames = prefix.getSchemaName() == null
                ? listTables(session, (String) null)
                : ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));

        return tableNames.stream()
                .collect(Collectors.toMap(
                        Function.identity(),
                        (tableName) -> getTableMetadata(tableName).getColumns())
                );
    }

    /**
     * Return column metadata
     * @param session
     * @param tableHandle
     * @param columnHandle
     * @return
     */
    @Override
    public ColumnMetadata getColumnMetadata(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            ColumnHandle columnHandle
    ) {
        return convertColumnHandle(columnHandle).getColumnMetadata();
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle) {
        return new ConnectorTableLayout(handle);
    }

    /**
     * Return the columns and their types
     * @param session
     * @param tableHandle table to get columns of
     * @return list of columns
     */
    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle) {
        return getTableMetadata(convertTableHandle(tableHandle).toSchemaTableName());
    }

    /**
     * Return the columns and their types
     * @param schemaTableName table to get columns of
     * @return list of columns
     */
    private ConnectorTableMetadata getTableMetadata(SchemaTableName schemaTableName) {
        List<ColumnMetadata> columnMetadata = getColumnsWithTypes(schemaTableName.getTableName()).stream()
                .map(column -> new ColumnMetadata(column.first, column.second))
                .collect(Collectors.toList());

        return new ConnectorTableMetadata(schemaTableName, columnMetadata);
    }

    /**
     * Return the columns and their types
     * @param session
     * @param tableHandle table to get columns of
     * @return list of columns
     */
    @SuppressWarnings("ValueOfIncrementOrDecrementUsed")
    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
        AtomicInteger index = new AtomicInteger();
        return getColumnsWithTypes(convertTableHandle(tableHandle).getTableName()).stream()
                .map(column -> new TezosColumnHandle(index.getAndIncrement(), column.first, column.second))
                .collect(Collectors.toMap(TezosColumnHandle::getName, Function.identity()));
    }

    /**
     * Return the columns and their types
     * @param table table to get columns of
     * @return list of columns
     */
    abstract protected List<Pair<String, Type>> getColumnsWithTypes(String table);
}
