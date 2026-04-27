#!/usr/bin/env bash
# Probe: iotax_getStakesByIds and iotax_getTimelockedStakesByIds with
# unusual object IDs. These paths previously contained
# `version.one_before().unwrap()` for deleted objects.
set -u
HERE="$(dirname "$0")"
. "$HERE/lib.sh"

probe() { run_probe "stakes/$1" "$2" || return 1; }

# Empty list
probe "empty_list" '{
  "jsonrpc":"2.0","id":1,"method":"iotax_getStakesByIds","params":[[]]
}'

# Single nonexistent object
probe "nonexistent_obj" '{
  "jsonrpc":"2.0","id":1,"method":"iotax_getStakesByIds",
  "params":[["0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"]]
}'

# Many duplicates
ids=$(python3 -c 'import sys;sys.stdout.write(",".join(["\"0x0000000000000000000000000000000000000000000000000000000000000001\""]*1000))')
probe "many_duplicates" '{
  "jsonrpc":"2.0","id":1,"method":"iotax_getStakesByIds","params":[['"$ids"']]
}'

# Timelocked variant
probe "timelocked_nonexistent" '{
  "jsonrpc":"2.0","id":1,"method":"iotax_getTimelockedStakesByIds",
  "params":[["0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"]]
}'

# Reserved/system addresses
for sys_id in '0x0' '0x1' '0x2' '0x3' '0x5' '0x6'; do
  probe "system_id_${sys_id}" '{
    "jsonrpc":"2.0","id":1,"method":"iotax_getStakesByIds",
    "params":[["'"$sys_id"'"]]
  }'
done
