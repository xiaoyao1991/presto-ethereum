package im.xiaoyao.presto.ethereum;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.PageBuilderStatus;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.RecordCursor;
import com.google.common.collect.ImmutableList;
import im.xiaoyao.presto.ethereum.handle.EthereumColumnHandle;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.joda.time.DateTimeZone;
import org.web3j.protocol.core.methods.response.EthBlock;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.Chars.isCharType;
import static com.facebook.presto.common.type.Chars.truncateToLengthAndTrimSpaces;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.Varchars.isVarcharType;
import static com.facebook.presto.common.type.Varchars.truncateToLength;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Float.floatToRawIntBits;
import static java.util.Objects.requireNonNull;

public abstract class BaseEthereumRecordCursor implements RecordCursor {
    protected final List<EthereumColumnHandle> columnHandles;
    protected final int[] fieldToColumnIndex;

    protected List<Supplier> suppliers;

    public BaseEthereumRecordCursor(List<EthereumColumnHandle> columnHandles) {
        this.columnHandles = columnHandles;
        this.suppliers = Collections.emptyList();

        fieldToColumnIndex = new int[columnHandles.size()];
        for (int i = 0; i < columnHandles.size(); i++) {
            EthereumColumnHandle columnHandle = columnHandles.get(i);
            fieldToColumnIndex[i] = columnHandle.getOrdinalPosition();
        }
    }

    @Override
    public Type getType(int field) {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getType();
    }

    @Override
    public long getReadTimeNanos() {
        return 0;
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
            currentBuilder = elementType.createBlockBuilder(new PageBuilderStatus().createBlockBuilderStatus(), list.size());
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
        boolean builderSynthesized = false;

        if (builder == null) {
            builderSynthesized = true;
            builder = type.createBlockBuilder(new PageBuilderStatus().createBlockBuilderStatus(), 1);
        }
        BlockBuilder currentBuilder = builder.beginBlockEntry();

        for (Map.Entry<?, ?> entry : map.entrySet()) {
            // Hive skips map entries with null keys
            if (entry.getKey() != null) {
                serializeObject(keyType, currentBuilder, entry.getKey());
                serializeObject(valueType, currentBuilder, entry.getValue());
            }
        }

        builder.closeEntry();
        if (builderSynthesized) {
            return (Block) type.getObject(builder, 0);
        } else {
            return null;
        }
    }

    private static Block serializeStruct(Type type, BlockBuilder builder, Object object) {
        if (object == null) {
            requireNonNull(builder, "parent builder is null").appendNull();
            return null;
        }

        List<Type> typeParameters = type.getTypeParameters();
        EthBlock.TransactionObject structData = (EthBlock.TransactionObject) object;
        boolean builderSynthesized = false;
        if (builder == null) {
            builderSynthesized = true;
            builder = type.createBlockBuilder(new PageBuilderStatus().createBlockBuilderStatus(), 1);
        }
        BlockBuilder currentBuilder = builder.beginBlockEntry();

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

        builder.closeEntry();
        if (builderSynthesized) {
            return (Block) type.getObject(builder, 0);
        } else {
            return null;
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
            sliceValue = truncateToLengthAndTrimSpaces(sliceValue, type);
        }

        return sliceValue;
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
}
