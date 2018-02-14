package im.xiaoyao.presto.ethereum;

import io.airlift.log.Logger;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.methods.response.EthBlock;
import org.web3j.protocol.core.methods.response.Log;
import org.web3j.protocol.core.methods.response.TransactionReceipt;

import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;

/**
 * Created by xiaoyaoqian on 2/14/18.
 */
public class EthereumLogLazyIterator implements Iterator<Log> {
    private static final Logger log = Logger.get(EthereumRecordCursor.class);

    private final Iterator<EthBlock.TransactionResult> txIter;
    private Iterator<Log> logIter;
    private final Web3j web3j;

    public EthereumLogLazyIterator(EthBlock block, Web3j web3j) {
        this.txIter = block.getBlock().getTransactions().iterator();
        this.web3j = web3j;
    }

    @Override
    public boolean hasNext() {
        if (logIter != null && logIter.hasNext()) {
            return true;
        }

        while (txIter.hasNext()) {
            EthBlock.TransactionResult tr = txIter.next();
            EthBlock.TransactionObject tx = (EthBlock.TransactionObject) tr.get();
            try {
                log.info("Getting tx receipts...");
                Optional<TransactionReceipt> transactionReceiptOptional = web3j.ethGetTransactionReceipt(tx.getHash())
                        .send()
                        .getTransactionReceipt()
                        .filter(receipt -> receipt.getLogs() != null && !receipt.getLogs().isEmpty());
                if (!transactionReceiptOptional.isPresent()) {
                    continue;
                }

                this.logIter = transactionReceiptOptional.get().getLogs().iterator();
                return true;
            } catch (IOException e) {
                throw new IllegalStateException("Unable to get transactionReceipt");
            }

        }

        return false;
    }

    @Override
    public Log next() {
        return logIter.next();
    }
}
