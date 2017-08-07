# Presto Ethereum Connector
Unleash the Power of Presto Interactive SQL Querying on Ethereum Blockchain

### Introduction
[Presto](https://prestodb.io) is a powerful interactive querying engine that enables running SQL queries on anything -- be it MySQL, HDFS, local file, Kafka -- as long as there exist a connector to the source.

This is a Presto connector to the Ethereum blockchain data. With this connector, one can get hands on with Ethereum blockchain analytics work without having to know how to play with the nitty gritty Javascript API.

### Prerequisites
Have an Ethereum client that you can connect to. There are 2 options:
1. Run [Geth](https://github.com/ethereum/go-ethereum) or [Parity](https://github.com/paritytech/parity) locally.
1. Use [Infura](https://infura.io), a hosted Ethereum client in the cloud.    

### Usage
1. [Install Presto](https://github.com/prestodb/presto/blob/master/README.md). *Follow the instructions on that page to create relevant config files.*  
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
1. [Install Presto CLI](https://prestodb.io/docs/current/installation/cli.html)
1. Clone this repo and run `mvn clean package` to build the plugin. You will find the built plugin in the `target` folder.
1. Load the plugin to Presto  
    a. Create the ethereum connector config inside of `etc`.  
    `$ mkdir -p etc/catalog && touch etc/catalog/ethereum.properties`   
    Paste the following to the ethereum.properties:
    ```
    connector.name=ethereum

    # You can connect through Ethereum HTTP JSON RPC endpoint
    ethereum.jsonrpc=http://localhost:8545/

    # Or you can connect through IPC socket
    # ethereum.ipc=/path/to/ipc_socketfile

    # Or you can connect to Infura
    # ethereum.infura=https://mainnet.infura.io/<your_token>
    ```
    b. Copy and extract the built plugin to your presto plugin folder  
    ```
    $ mkdir -p plugin/ethereum \
      && cp <path_to_this_repo>/target/presto-ethereum-*-plugin.tar.gz . \
      && tar xfz presto-ethereum-*-plugin.tar.gz -C plugin/ethereum --strip-components=1
    ```  

    By the end of this step, your presto installation folder structure should look like:  
      ```
      ├── bin
      ├── lib
      ├── etc
      │   ├── catalog
      │   │   └── ethereum.properties
      │   ├── config.properties
      │   ├── jvm.config
      │   └── node.properties
      ├── plugin
      │   ├── ethereum
      │   │   └── <some jars>
      ```
1. There you go. You can now start the presto server, and query through presto-cli:  
  ```
  $ bin/launcher start
  $ presto-cli --server localhost:8080 --catalog ethereum --schema default
  ```

### Use Cases
Inspired by [An Analysis of the First 100000 Blocks](https://blog.ethereum.org/2015/08/18/frontier-first-100k-blocks/), the following SQL queries capture partially what was depicted in that post.  

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

### Web3 Functions
In addition to the various built-in [Presto functions](https://prestodb.io/docs/current/functions.html), some web3 functions are ported so that they can be called inline with SQL statements directly. Currently, the supported web3 functions are
1. [fromWei](https://github.com/ethereum/wiki/wiki/JavaScript-API#web3fromwei)
1. [toWei](https://github.com/ethereum/wiki/wiki/JavaScript-API#web3towei)
1. [eth_gasPrice](https://github.com/ethereum/wiki/wiki/JavaScript-API#web3ethgasprice)
1. [eth_blockNumber](https://github.com/ethereum/wiki/wiki/JavaScript-API#web3ethblocknumber)
1. [eth_getBalance](https://github.com/ethereum/wiki/wiki/JavaScript-API#web3ethgetbalance)
1. [eth_getTransactionCount](https://github.com/ethereum/wiki/wiki/JavaScript-API#web3ethgettransactioncount)
