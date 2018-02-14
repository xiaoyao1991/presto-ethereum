package im.xiaoyao.presto.ethereum;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.InterleavedBlockBuilder;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.joda.time.DateTimeZone;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.methods.response.EthBlock;
import org.web3j.protocol.core.methods.response.Log;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.Chars.isCharType;
import static com.facebook.presto.spi.type.Chars.trimSpacesAndTruncateToLength;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.facebook.presto.spi.type.Varchars.truncateToLength;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Float.floatToRawIntBits;
import static java.util.Objects.requireNonNull;

public class EthereumRecordCursor implements RecordCursor {
    private static final Logger log = Logger.get(EthereumRecordCursor.class);

    private final EthBlock block;
    private final Iterator<EthBlock> blockIter;
    private final Iterator<EthBlock.TransactionResult> txIter;
    private final Iterator<Log> logIter;

    private final EthereumTable table;
    private final Web3j web3j;

    private final List<EthereumColumnHandle> columnHandles;
    private final int[] fieldToColumnIndex;

    private List<Supplier> suppliers;

    public EthereumRecordCursor(List<EthereumColumnHandle> columnHandles, EthBlock block, EthereumTable table, Web3j web3j) {
        this.columnHandles = columnHandles;
        this.table = table;
        this.web3j = web3j;
        this.suppliers = Collections.emptyList();

        fieldToColumnIndex = new int[columnHandles.size()];
        for (int i = 0; i < columnHandles.size(); i++) {
            EthereumColumnHandle columnHandle = columnHandles.get(i);
            fieldToColumnIndex[i] = columnHandle.getOrdinalPosition();
        }

        // TODO: handle failure upstream
        this.block = requireNonNull(block, "block is null");
        this.blockIter = ImmutableList.of(block).iterator();
        this.txIter = block.getBlock().getTransactions().iterator();
        this.logIter = new EthereumLogLazyIterator(block, web3j);
    }

    @Override
    public long getTotalBytes() {
        return block.getBlock().getSize().longValue();
    }

    @Override
    public long getCompletedBytes() {
        return block.getBlock().getSize().longValue();
    }

    @Override
    public long getReadTimeNanos() {
        return 0;
    }

    @Override
    public Type getType(int field) {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getType();
    }

    @Override
    public boolean advanceNextPosition() {
        if (table == EthereumTable.BLOCK && !blockIter.hasNext()
                || table == EthereumTable.TRANSACTION && !txIter.hasNext()
                || table == EthereumTable.ERC20 && !logIter.hasNext()) {
            return false;
        }

        ImmutableList.Builder<Supplier> builder = ImmutableList.builder();
        if (table == EthereumTable.BLOCK) {
            blockIter.next();
            EthBlock.Block blockBlock = this.block.getBlock();
            builder.add(blockBlock::getNumber);
            builder.add(blockBlock::getHash);
            builder.add(blockBlock::getParentHash);
            builder.add(blockBlock::getNonceRaw);
            builder.add(blockBlock::getSha3Uncles);
            builder.add(blockBlock::getLogsBloom);
            builder.add(blockBlock::getTransactionsRoot);
            builder.add(blockBlock::getStateRoot);
            builder.add(blockBlock::getMiner);
            builder.add(blockBlock::getDifficulty);
            builder.add(blockBlock::getTotalDifficulty);
            builder.add(blockBlock::getSize);
            builder.add(blockBlock::getExtraData);
            builder.add(blockBlock::getGasLimit);
            builder.add(blockBlock::getGasUsed);
            builder.add(blockBlock::getTimestamp);
            builder.add(() -> {
                return blockBlock.getTransactions()
                        .stream()
                        .map(tr -> ((EthBlock.TransactionObject) tr.get()).getHash())
                        .collect(Collectors.toList());
            });
            builder.add(blockBlock::getUncles);

        } else if (table == EthereumTable.TRANSACTION) {
            EthBlock.TransactionResult tr = txIter.next();
            EthBlock.TransactionObject tx = (EthBlock.TransactionObject) tr.get();

            builder.add(tx::getHash);
            builder.add(tx::getNonce);
            builder.add(tx::getBlockHash);
            builder.add(tx::getBlockNumber);
            builder.add(tx::getTransactionIndex);
            builder.add(tx::getFrom);
            builder.add(tx::getTo);
            builder.add(tx::getValue);
            builder.add(tx::getGas);
            builder.add(tx::getGasPrice);
            builder.add(tx::getInput);
        } else if (table == EthereumTable.ERC20) {
            while (logIter.hasNext()) {
                Log l = logIter.next();
                if (l.getTopics().get(0).equalsIgnoreCase(EthereumERC20Utils.TRANSFER_EVENT_TOPIC)) {
                    // Token contract address
                    builder.add(() -> Optional.ofNullable(EthereumERC20Token.lookup.get(l.getAddress().toLowerCase()))
                            .map(Enum::name).orElse(String.format("Unknown ERC20 Token(%s)", l.getAddress())));
                    // from address
                    builder.add(() -> h32ToH20(l.getTopics().get(1)));
                    // to address
                    builder.add(() -> h32ToH20(l.getTopics().get(2)));
                    // amount value
                    builder.add(() -> EthereumERC20Utils.hexToDouble(l.getData()));
                    builder.add(l::getTransactionHash);
                    builder.add(l::getBlockNumber);
                    this.suppliers = builder.build();
                    return true;
                }
            }

            return false;
        } else {
            return false;
        }

        this.suppliers = builder.build();
        return true;
    }

    @Override
    public boolean getBoolean(int field) {
        return (boolean) suppliers.get(fieldToColumnIndex[field]).get();
    }

    @Override
    public long getLong(int field) {
        return ((Number) suppliers.get(fieldToColumnIndex[field]).get()).longValue();
    }

    @Override
    public double getDouble(int field) {
        return ((Number) suppliers.get(fieldToColumnIndex[field]).get()).doubleValue();
    }

    @Override
    public Slice getSlice(int field) {
        return Slices.utf8Slice((String) suppliers.get(fieldToColumnIndex[field]).get());
    }

    @Override
    public Object getObject(int field) {
        return serializeObject(columnHandles.get(field).getType(), null, suppliers.get(fieldToColumnIndex[field]).get());
    }

    @Override
    public boolean isNull(int field) {
        return suppliers.get(fieldToColumnIndex[field]).get() == null;
    }

    @Override
    public void close() {
    }

    private static long getLongExpressedValue(Object value) {
        if (value instanceof Date) {
            long storageTime = ((Date) value).getTime();
            // convert date from VM current time zone to UTC
            long utcMillis = storageTime + DateTimeZone.getDefault().getOffset(storageTime);
            return TimeUnit.MILLISECONDS.toDays(utcMillis);
        }
        if (value instanceof Timestamp) {
            long parsedJvmMillis = ((Timestamp) value).getTime();
            DateTimeZone jvmTimeZone = DateTimeZone.getDefault();
            long convertedMillis = jvmTimeZone.convertUTCToLocal(parsedJvmMillis);

            return convertedMillis;
        }
        if (value instanceof Float) {
            return floatToRawIntBits(((Float) value));
        }
        return ((Number) value).longValue();
    }

    private static Slice getSliceExpressedValue(Object value, Type type) {
        Slice sliceValue;
        if (value instanceof String) {
            sliceValue = Slices.utf8Slice((String) value);
        } else if (value instanceof byte[]) {
            sliceValue = Slices.wrappedBuffer((byte[]) value);
        } else if (value instanceof Integer) {
            sliceValue = Slices.utf8Slice(value.toString());
        } else {
            throw new IllegalStateException("unsupported string field type: " + value.getClass().getName());
        }
        if (isVarcharType(type)) {
            sliceValue = truncateToLength(sliceValue, type);
        }
        if (isCharType(type)) {
            sliceValue = trimSpacesAndTruncateToLength(sliceValue, type);
        }

        return sliceValue;
    }

    private static Block serializeObject(Type type, BlockBuilder builder, Object object) {
        if (!isStructuralType(type)) {
            serializePrimitive(type, builder, object);
            return null;
        } else if (isArrayType(type)) {
            return serializeList(type, builder, object);
        } else if (isMapType(type)) {
            return serializeMap(type, builder, object);
        } else if (isRowType(type)) {
            return serializeStruct(type, builder, object);
        }
        throw new RuntimeException("Unknown object type: " + type);
    }

    private static Block serializeList(Type type, BlockBuilder builder, Object object) {
        List<?> list = (List) object;
        if (list == null) {
            requireNonNull(builder, "parent builder is null").appendNull();
            return null;
        }

        List<Type> typeParameters = type.getTypeParameters();
        checkArgument(typeParameters.size() == 1, "list must have exactly 1 type parameter");
        Type elementType = typeParameters.get(0);

        BlockBuilder currentBuilder;
        if (builder != null) {
            currentBuilder = builder.beginBlockEntry();
        } else {
            currentBuilder = elementType.createBlockBuilder(new BlockBuilderStatus(), list.size());
        }

        for (Object element : list) {
            serializeObject(elementType, currentBuilder, element);
        }

        if (builder != null) {
            builder.closeEntry();
            return null;
        } else {
            Block resultBlock = currentBuilder.build();
            return resultBlock;
        }
    }

    private static Block serializeMap(Type type, BlockBuilder builder, Object object) {
        Map<?, ?> map = (Map) object;
        if (map == null) {
            requireNonNull(builder, "parent builder is null").appendNull();
            return null;
        }

        List<Type> typeParameters = type.getTypeParameters();
        checkArgument(typeParameters.size() == 2, "map must have exactly 2 type parameter");
        Type keyType = typeParameters.get(0);
        Type valueType = typeParameters.get(1);

        BlockBuilder currentBuilder;
        if (builder != null) {
            currentBuilder = builder.beginBlockEntry();
        } else {
            currentBuilder = new InterleavedBlockBuilder(typeParameters, new BlockBuilderStatus(), map.size());
        }

        for (Map.Entry<?, ?> entry : map.entrySet()) {
            // Hive skips map entries with null keys
            if (entry.getKey() != null) {
                serializeObject(keyType, currentBuilder, entry.getKey());
                serializeObject(valueType, currentBuilder, entry.getValue());
            }
        }

        if (builder != null) {
            builder.closeEntry();
            return null;
        } else {
            Block resultBlock = currentBuilder.build();
            return resultBlock;
        }
    }

    private static Block serializeStruct(Type type, BlockBuilder builder, Object object) {
        if (object == null) {
            requireNonNull(builder, "parent builder is null").appendNull();
            return null;
        }

        List<Type> typeParameters = type.getTypeParameters();
        EthBlock.TransactionObject structData = (EthBlock.TransactionObject) object;
        BlockBuilder currentBuilder;
        if (builder != null) {
            currentBuilder = builder.beginBlockEntry();
        } else {
            currentBuilder = new InterleavedBlockBuilder(typeParameters, new BlockBuilderStatus(), typeParameters.size());
        }

        ImmutableList.Builder<Supplier> lstBuilder = ImmutableList.builder();
        lstBuilder.add(structData::getHash);
        lstBuilder.add(structData::getNonce);
        lstBuilder.add(structData::getBlockHash);
        lstBuilder.add(structData::getBlockNumber);
        lstBuilder.add(structData::getTransactionIndex);
        lstBuilder.add(structData::getFrom);
        lstBuilder.add(structData::getTo);
        lstBuilder.add(structData::getValue);
        lstBuilder.add(structData::getGas);
        lstBuilder.add(structData::getGasPrice);
        lstBuilder.add(structData::getInput);
        ImmutableList<Supplier> txColumns = lstBuilder.build();

        for (int i = 0; i < typeParameters.size(); i++) {
            serializeObject(typeParameters.get(i), currentBuilder, txColumns.get(i).get());
        }

        if (builder != null) {
            builder.closeEntry();
            return null;
        } else {
            Block resultBlock = currentBuilder.build();
            return resultBlock;
        }
    }

    private static void serializePrimitive(Type type, BlockBuilder builder, Object object) {
        requireNonNull(builder, "parent builder is null");

        if (object == null) {
            builder.appendNull();
            return;
        }

        if (BOOLEAN.equals(type)) {
            BOOLEAN.writeBoolean(builder, (Boolean) object);
        } else if (BIGINT.equals(type) || INTEGER.equals(type) || SMALLINT.equals(type) || TINYINT.equals(type)
                || REAL.equals(type) || DATE.equals(type) || TIMESTAMP.equals(type)) {
            type.writeLong(builder, getLongExpressedValue(object));
        } else if (DOUBLE.equals(type)) {
            DOUBLE.writeDouble(builder, ((Number) object).doubleValue());
        } else if (isVarcharType(type) || VARBINARY.equals(type) || isCharType(type)) {
            type.writeSlice(builder, getSliceExpressedValue(object, type));
        } else {
            throw new UnsupportedOperationException("Unsupported primitive type: " + type);
        }
    }

    public static boolean isArrayType(Type type) {
        return type.getTypeSignature().getBase().equals(StandardTypes.ARRAY);
    }

    public static boolean isMapType(Type type) {
        return type.getTypeSignature().getBase().equals(StandardTypes.MAP);
    }

    public static boolean isRowType(Type type) {
        return type.getTypeSignature().getBase().equals(StandardTypes.ROW);
    }

    public static boolean isStructuralType(Type type) {
        String baseName = type.getTypeSignature().getBase();
        return baseName.equals(StandardTypes.MAP) || baseName.equals(StandardTypes.ARRAY) || baseName.equals(StandardTypes.ROW);
    }

    private static String h32ToH20(String h32) {
        return "0x" + h32.substring(EthereumMetadata.H32_BYTE_HASH_STRING_LENGTH - EthereumMetadata.H20_BYTE_HASH_STRING_LENGTH + 2);
    }
}
