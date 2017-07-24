package im.xiaoyao.presto.ethereum;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.web3j.protocol.core.methods.response.EthBlock;

import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class EthereumRecordCursor implements RecordCursor {
    private final EthBlock block;
    private final Iterator<EthBlock> iter;

    private final List<EthereumColumnHandle> columnHandles;
    private final int[] fieldToColumnIndex;

    private List<Supplier> suppliers;

    public EthereumRecordCursor(List<EthereumColumnHandle> columnHandles, EthBlock block) {
        this.columnHandles = columnHandles;

        fieldToColumnIndex = new int[columnHandles.size()];
        for (int i = 0; i < columnHandles.size(); i++) {
            EthereumColumnHandle columnHandle = columnHandles.get(i);
            fieldToColumnIndex[i] = columnHandle.getOrdinalPosition();
        }

        // TODO: handle failure upstream
        this.block = requireNonNull(block, "block is null");
        this.iter = ImmutableList.of(block).iterator();
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
        if (!iter.hasNext()) {
            return false;
        }

        iter.next();

        ImmutableList.Builder<Supplier> builder = ImmutableList.builder();

        builder.add(() -> {
            return block.getBlock().getNumber();
        });
        builder.add(() -> {
            return block.getBlock().getHash();
        });
        builder.add(() -> {
            return block.getBlock().getParentHash();
        });
        builder.add(() -> {
            return block.getBlock().getNonceRaw();
        });
        builder.add(() -> {
            return block.getBlock().getSha3Uncles();
        });
        builder.add(() -> {
            return block.getBlock().getLogsBloom();
        });
        builder.add(() -> {
            return block.getBlock().getTransactionsRoot();
        });
        builder.add(() -> {
            return block.getBlock().getStateRoot();
        });
        builder.add(() -> {
            return block.getBlock().getMiner();
        });
        builder.add(() -> {
            return block.getBlock().getDifficulty();
        });
        builder.add(() -> {
            return block.getBlock().getTotalDifficulty();
        });
        builder.add(() -> {
            return block.getBlock().getSize();
        });
        builder.add(() -> {
            return block.getBlock().getExtraData();
        });
        builder.add(() -> {
            return block.getBlock().getGasLimit();
        });
        builder.add(() -> {
            return block.getBlock().getGasUsed();
        });
        builder.add(() -> {
            return block.getBlock().getTimestamp();
        });
        builder.add(() -> {
            return block.getBlock().getTransactions();
        });
        builder.add(() -> {
            return block.getBlock().getUncles();
        });

        this.suppliers = builder.build();

        return true;
    }

    @Override
    public boolean getBoolean(int field) {
        return false;
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
        return null;
    }

    @Override
    public boolean isNull(int field) {
        return suppliers.get(fieldToColumnIndex[field]).get() == null;
    }

    @Override
    public void close() {
    }
}
