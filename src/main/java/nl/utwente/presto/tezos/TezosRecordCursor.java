package nl.utwente.presto.tezos;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import nl.utwente.presto.tezos.erc20.TezosERC20Token;
import nl.utwente.presto.tezos.erc20.TezosERC20Utils;
import nl.utwente.presto.tezos.handle.TezosColumnHandle;
import io.airlift.log.Logger;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.methods.response.EthBlock;
import org.web3j.protocol.core.methods.response.Log;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class TezosRecordCursor extends BaseTezosRecordCursor {
    private static final Logger log = Logger.get(TezosRecordCursor.class);

    private final EthBlock block;
    private final Iterator<EthBlock> blockIter;
    private final Iterator<EthBlock.TransactionResult> txIter;
    private final Iterator<Log> logIter;

    private final TezosTable table;

    public TezosRecordCursor(List<TezosColumnHandle> columnHandles, EthBlock block, TezosTable table, Web3j web3j) {
        super(columnHandles);

        this.table = table;

        // TODO: handle failure upstream
        this.block = requireNonNull(block, "block is null");
        this.blockIter = ImmutableList.of(block).iterator();
        this.txIter = block.getBlock().getTransactions().iterator();
        this.logIter = new TezosLogLazyIterator(block, web3j);
    }

    @Override
    public long getCompletedBytes() {
        return block.getBlock().getSize().longValue();
    }

    @Override
    public boolean advanceNextPosition() {
        if (table == TezosTable.BLOCK && !blockIter.hasNext()
                || table == TezosTable.TRANSACTION && !txIter.hasNext()
                || table == TezosTable.ERC20 && !logIter.hasNext()) {
            return false;
        }

        ImmutableList.Builder<Supplier> builder = ImmutableList.builder();
        if (table == TezosTable.BLOCK) {
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

        } else if (table == TezosTable.TRANSACTION) {
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
        } else if (table == TezosTable.ERC20) {
            while (logIter.hasNext()) {
                Log l = logIter.next();
                List<String> topics = l.getTopics();
                String data = l.getData();

                if (topics.get(0).equalsIgnoreCase(TezosERC20Utils.TRANSFER_EVENT_TOPIC)) {
                    // Handle unindexed event fields:
                    // if the number of topics and fields in data part != 4, then it's a weird event
                    if (topics.size() < 3 && topics.size() + (data.length() - 2) / 64 != 4) {
                        continue;
                    }

                    if (topics.size() < 3) {
                        Iterator<String> dataFields = Splitter.fixedLength(64).split(data.substring(2)).iterator();
                        while (topics.size() < 3) {
                            topics.add("0x" + dataFields.next());
                        }
                        data = "0x" + dataFields.next();
                    }

                    // Token contract address
                    builder.add(() -> Optional.ofNullable(TezosERC20Token.lookup.get(l.getAddress().toLowerCase()))
                            .map(Enum::name).orElse(String.format("ERC20(%s)", l.getAddress())));
                    // from address
                    builder.add(() -> h32ToH20(topics.get(1)));
                    // to address
                    builder.add(() -> h32ToH20(topics.get(2)));
                    // amount value
                    String finalData = data;
                    builder.add(() -> TezosERC20Utils.hexToDouble(finalData));
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

    private static String h32ToH20(String h32) {
        return "0x" + h32.substring(TezosMetadata.H32_BYTE_HASH_STRING_LENGTH - TezosMetadata.H20_BYTE_HASH_STRING_LENGTH + 2);
    }
}
