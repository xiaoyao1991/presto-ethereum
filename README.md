# Presto Tezos Connector
Unleash the Power of Presto Interactive SQL Querying on Tezos Blockchain

### Introduction
[Presto](https://prestosql.io) is a powerful interactive querying engine that enables running SQL queries on anything -- be it MySQL, HDFS, local file, Kafka -- as long as there exist a connector to the source.

This is a Presto connector to the Tezos blockchain. With this connector, one can query information from the Tezos network from the comforts of SQL.

### Prerequisites
TODO
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
