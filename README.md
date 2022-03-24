# Presto Tezos Connector
Unleash the Power of Presto Interactive SQL Querying on Tezos Blockchain

### Introduction
[Presto](https://prestosql.io) is a powerful interactive querying engine that enables running SQL queries on anything -- be it MySQL, HDFS, local file, Kafka -- as long as there exist a connector to the source.

This is a Presto connector to the Tezos blockchain data. With this connector, one can get hands on with Tezos blockchain analytics work without having to know how to play with the nitty gritty Javascript API.

### Prerequisites
Have an Tezos client that you can connect to. There are 2 options:
1. Run [Geth](https://github.com/tezos/go-tezos) or [Parity](https://github.com/paritytech/parity) locally.
1. Use [Infura](https://infura.io), a hosted Tezos client in the cloud.    

### Note
Specify a block range where you can (e.g. `WHERE block.block_number > x AND block.block_number < y`, or `WHERE transaction.tx_blocknumber > x AND transaction.tx_blocknumber < y`, or `WHERE erc20.erc20_blocknumber > x AND erc20.erc20_blocknumber < y`). Block number is the default and only predicate that can push down to narrow down data scan range. Queries without block ranges will cause presto to retrieve blocks all the way from the first block, which takes forever. 

### Usage
1. [Install Presto](https://prestosql.io/docs/current/installation/deployment.html). *Follow the instructions on that page to create relevant config files.*  
  By the end of this step, your presto installation folder structure should look like:
    ```
    ├── bin
    ├── lib
    ├── etc
    │   ├── config.properties
    │   ├── jvm.config
    │   └── node.properties
    ├── plugin
    ```
1. [Install Presto CLI](https://prestosql.io/docs/current/installation/cli.html)
1. Clone this repo and run `mvn clean package` to build the plugin. You will find the built plugin in the `target` folder.
1. Load the plugin to Presto  
    a. Create the tezos connector config inside of `etc`.  
    `$ mkdir -p etc/catalog && touch etc/catalog/tezos.properties`   
    Paste the following to the tezos.properties:
    ```
    connector.name=tezos

    # You can connect through Tezos HTTP JSON RPC endpoint
    # IMPORTANT - for local testing start geth with rpcport
    # geth --rpc --rpcaddr "127.0.0.1" --rpcport "8545"
    tezos.jsonrpc=http://localhost:8545/


    # Or you can connect through IPC socket
    # tezos.ipc=/path/to/ipc_socketfile

    # Or you can connect to Infura
    # tezos.infura=https://mainnet.infura.io/<your_token>
    ```
    b. Copy and extract the built plugin to your presto plugin folder  
    ```
    $ mkdir -p plugin/tezos \
      && cp <path_to_this_repo>/target/presto-tezos-*-plugin.tar.gz . \
      && tar xfz presto-tezos-*-plugin.tar.gz -C plugin/tezos --strip-components=1
    ```  

    By the end of this step, your presto installation folder structure should look like:  
      ```
      ├── bin
      ├── lib
      ├── etc
      │   ├── catalog
      │   │   └── tezos.properties
      │   ├── config.properties
      │   ├── jvm.config
      │   └── node.properties
      ├── plugin
      │   ├── tezos
      │   │   └── <some jars>
      ```
1. There you go. You can now start the presto server, and query through presto-cli:  
  ```
  $ bin/launcher start
  $ presto-cli --server localhost:8080 --catalog tezos --schema default
  ```

### Use Cases
Inspired by [An Analysis of the First 100000 Blocks](https://blog.tezos.org/2015/08/18/frontier-first-100k-blocks/), the following SQL queries capture partially what was depicted in that post.  

- The first 50 block times (in seconds)
```sql
SELECT b.bn, (b.block_timestamp - a.block_timestamp) AS delta
FROM
    (SELECT block_number AS bn, block_timestamp
    FROM block
    WHERE block_number>=1 AND block_number<=50) AS a
JOIN
    (SELECT (block_number-1) AS bn, block_timestamp
    FROM block
    WHERE block_number>=2 AND block_number<=51) AS b
ON a.bn=b.bn
ORDER BY b.bn;
```
- Average block time (every 200th block from genesis to block 10000)
```sql
WITH
X AS (SELECT b.bn, (b.block_timestamp - a.block_timestamp) AS delta
        FROM
            (SELECT block_number AS bn, block_timestamp
            FROM block
            WHERE block_number>=1 AND block_number<=10000) AS a
        JOIN
            (SELECT (block_number-1) AS bn, block_timestamp
            FROM block
            WHERE block_number>=2 AND block_number<=10001) AS b
        ON a.bn=b.bn
        ORDER BY b.bn)
SELECT min(bn) AS chunkStart, avg(delta)
FROM
    (SELECT ntile(10000/200) OVER (ORDER BY bn) AS chunk, * FROM X) AS T
GROUP BY chunk
ORDER BY chunkStart;
```
- Biggest miners in first 100k blocks (address, blocks, %)
```sql
SELECT block_miner, count(*) AS num, count(*)/100000.0 AS PERCENT
FROM block
WHERE block_number<=100000
GROUP BY block_miner
ORDER BY num DESC
LIMIT 15;
```
- ERC20 Token Movement in the last 100 blocks
```sql
SELECT erc20_token, SUM(erc20_value) FROM erc20
WHERE erc20_blocknumber >= 4147340 AND erc20_blocknumber<=4147350
GROUP BY erc20_token;
```
- Describe the database structure
```sql
SHOW TABLES;
    Table
-------------
 block
 erc20
 transaction

DESCRIBE block;
Column                 | Type               | Extra | Comment
-----------------------------------------------------------
block_number           | bigint             |       |
block_hash             | varchar(66)        |       |
block_parenthash       | varchar(66)        |       |
block_nonce            | varchar(18)        |       |
block_sha3uncles       | varchar(66)        |       |
block_logsbloom        | varchar(514)       |       |
block_transactionsroot | varchar(66)        |       |
block_stateroot        | varchar(66)        |       |
block_miner            | varchar(42)        |       |
block_difficulty       | bigint             |       |
block_totaldifficulty  | bigint             |       |
block_size             | integer            |       |
block_extradata        | varchar            |       |
block_gaslimit         | double             |       |
block_gasused          | double             |       |
block_timestamp        | bigint             |       |
block_transactions     | array(varchar(66)) |       |
block_uncles           | array(varchar(66)) |       |


DESCRIBE transaction;

Column              |    Type     | Extra | Comment
--------------------------------------------------
tx_hash             | varchar(66) |       |
tx_nonce            | bigint      |       |
tx_blockhash        | varchar(66) |       |
tx_blocknumber      | bigint      |       |
tx_transactionindex | integer     |       |
tx_from             | varchar(42) |       |
tx_to               | varchar(42) |       |
tx_value            | double      |       |
tx_gas              | double      |       |
tx_gasprice         | double      |       |
tx_input            | varchar     |       |


DESCRIBE erc20;
      Column       |    Type     | Extra | Comment
-------------------+-------------+-------+---------
 erc20_token       | varchar     |       |
 erc20_from        | varchar(42) |       |
 erc20_to          | varchar(42) |       |
 erc20_value       | double      |       |
 erc20_txhash      | varchar(66) |       |
 erc20_blocknumber | bigint      |       |
```

### Web3 Functions
In addition to the various built-in [Presto functions](https://prestodb.io/docs/current/functions.html), some web3 functions are ported so that they can be called inline with SQL statements directly. Currently, the supported web3 functions are
1. [fromWei](https://github.com/tezos/wiki/wiki/JavaScript-API#web3fromwei)
1. [toWei](https://github.com/tezos/wiki/wiki/JavaScript-API#web3towei)
1. [eth_gasPrice](https://github.com/tezos/wiki/wiki/JavaScript-API#web3ethgasprice)
1. [eth_blockNumber](https://github.com/tezos/wiki/wiki/JavaScript-API#web3ethblocknumber)
1. [eth_getBalance](https://github.com/tezos/wiki/wiki/JavaScript-API#web3ethgetbalance)
1. [eth_getTransactionCount](https://github.com/tezos/wiki/wiki/JavaScript-API#web3ethgettransactioncount)

### Troubleshooting

* You must use python2. You will get invalid syntax errors if you use Python3.
```
-> bin/launcher start
  File "/your_path/presto-server-0.196/bin/launcher.py", line 38
    except OSError, e:
                  ^
SyntaxError: invalid syntax
```

* Use Java 8 only. You might get the following errors if you use the wrong Java version.

```
Unrecognized VM option 'ExitOnOutOfMemoryError'
Did you mean 'OnOutOfMemoryError=<value>'?
Error: Could not create the Java Virtual Machine.
Error: A fatal exception has occurred. Program will exit.
```
