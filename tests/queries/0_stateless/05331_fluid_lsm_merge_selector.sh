#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# This is a smoke test, it proves that the FluidLSM merge selector exists and does something.

${CLICKHOUSE_CLIENT} --query "
DROP TABLE IF EXISTS test;
CREATE TABLE test (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS merge_selector_algorithm = 'FluidLSM', merge_selector_fluid_lsm_k = 2, merge_selector_fluid_lsm_z = 1;
INSERT INTO test VALUES (1);
SELECT name, level FROM system.parts WHERE active AND table = 'test' AND database = currentDatabase() ORDER BY name;
INSERT INTO test VALUES (2);
INSERT INTO test VALUES (3);
INSERT INTO test VALUES (4);
INSERT INTO test VALUES (5);
OPTIMIZE TABLE test;
"

# The invalid bounds must be rejected.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_invalid (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS merge_selector_algorithm = 'FluidLSM', merge_selector_fluid_lsm_k = 0" 2>&1 | grep -o "BAD_ARGUMENTS" | head -n 1
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_invalid (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS merge_selector_algorithm = 'FluidLSM', merge_selector_fluid_lsm_z = 0" 2>&1 | grep -o "BAD_ARGUMENTS" | head -n 1

while true
do
    count=$(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.parts WHERE active AND table = 'test' AND database = currentDatabase() AND level > 0")
    [ "$count" != "0" ] && break
    sleep 0.1
done

${CLICKHOUSE_CLIENT} --query "
SELECT x FROM test ORDER BY x;
DROP TABLE test;
DROP TABLE IF EXISTS test_invalid;
"
