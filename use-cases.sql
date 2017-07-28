-- Inspired by https://blog.ethereum.org/2015/08/18/frontier-first-100k-blocks/
-- The following SQL queries capture partially what was depicted in that post.

-- The first 50 block times (in seconds):
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

-- Average block time (every 200th block from genesis to block 10000)
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

-- Biggest miners in first 100k blocks (address, blocks, %):
SELECT block_miner, count(*) AS num, count(*)/100000.0 AS PERCENT
FROM block
WHERE block_number<=100000
GROUP BY block_miner
ORDER BY num DESC
LIMIT 15;
