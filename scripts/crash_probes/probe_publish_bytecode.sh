#!/usr/bin/env bash
# Probe: submit malformed Move bytecode via iota_executeTransactionBlock
# (Publish command). The Move bytecode verifier and BCS deserializer must
# reject malformed bytecode without panicking the node.
#
# We sidestep the wallet/signing path by directly submitting a Publish
# command via dryRun (which doesn't require a signature).
set -u
HERE="$(dirname "$0")"
. "$HERE/lib.sh"

probe() { run_probe "publish/$1" "$2" || return 1; }

# Build a TransactionData wrapping a ProgrammableTransaction with one
# Command::Publish(modules, deps). We feed `modules` various malformed
# bytecode blobs.
build_publish_payload() {
  python3 - "$1" <<'PY'
import base64, sys
def uleb(n):
    out = bytearray()
    while True:
        b = n & 0x7f
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            break
    return bytes(out)

flavor = sys.argv[1]
if flavor == "empty":
    module_bytes = b""
elif flavor == "garbage":
    module_bytes = b"\x00" * 1024
elif flavor == "huge_zeros":
    module_bytes = b"\x00" * 5_000_000   # 5MB of zeros
elif flavor == "magic_only":
    # Move bytecode magic prefix without rest of the file
    module_bytes = b"\xa1\x1c\xeb\x0b\x07\x00\x00\x0a"
elif flavor == "trunc_after_magic":
    module_bytes = b"\xa1\x1c\xeb\x0b" + b"\x00" * 16
elif flavor == "high_version":
    # Magic + extremely high version field
    module_bytes = b"\xa1\x1c\xeb\x0b\xff\xff\xff\xff\xff\xff\xff\xff" + b"\x00" * 32
else:
    raise SystemExit("unknown flavor")

# TransactionData::V1 wrapper
b  = bytes([0x00])  # TransactionData::V1
# TransactionKind::ProgrammableTransaction
b += bytes([0x00])
# inputs: empty
b += bytes([0x00])
# commands: 1
b += bytes([0x01])
# Command::Publish(Vec<Vec<u8>>, Vec<ObjectID>)
b += bytes([0x04])  # Publish variant index
# Vec<Vec<u8>>: 1 module
b += bytes([0x01]) + uleb(len(module_bytes)) + module_bytes
# Vec<ObjectID>: 0 deps
b += bytes([0x00])
# sender, gas_data, expiration
b += b"\x00" * 32
# GasData: empty payment, owner=0..., price=1000 (valid RGP), budget=10_000_000_000
b += bytes([0x00])  # payment count
b += b"\x00" * 32   # owner
b += (1000).to_bytes(8, "little")          # price
b += (10_000_000_000).to_bytes(8, "little") # budget
# TransactionExpiration::None
b += bytes([0x00])
sys.stdout.write(base64.b64encode(b).decode())
PY
}

for flavor in empty garbage magic_only trunc_after_magic high_version; do
  payload=$(build_publish_payload "$flavor")
  probe "$flavor" '{
    "jsonrpc":"2.0","id":1,"method":"iota_dryRunTransactionBlock",
    "params":["'"$payload"'"]
  }'
done

# huge_zeros lives in its own probe because the body would exceed the
# server's 5MB limit when base64-encoded.
payload=$(build_publish_payload huge_zeros)
probe "huge_zeros" '{
  "jsonrpc":"2.0","id":1,"method":"iota_dryRunTransactionBlock",
  "params":["'"$payload"'"]
}'
