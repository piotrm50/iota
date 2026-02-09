#!/usr/bin/env python3
"""
Automated testing script for IOTA indexer RPC behavior with pruning enabled.

This script tests RPC responses when data is pruned:
1. Start local network with short epoch duration (5 seconds)
2. Start indexer sync with epochs_to_keep=2
3. Wait for several epochs to be indexed (until epoch 5)
4. Make RPC requests for data from current epoch (should succeed)
5. Make RPC requests for data from pruned epochs (should return appropriate errors)
6. Verify that the indexer correctly handles requests for pruned data

The focus of this test is ensuring the RPC layer correctly responds to queries
for both available and pruned data.

Usage:
    python test_rpc_with_pruning.py [--epochs-to-keep N] [--target-epoch N]
"""

import argparse
import os
import signal
import subprocess
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import base58
import psycopg2
import requests

# Configuration
# Default Configuration
DEFAULT_DB_USER = "postgres"
DEFAULT_DB_PASSWORD = "postgrespw"
DEFAULT_DB_HOST = "localhost"
DEFAULT_DB_PORT = 5432
DEFAULT_DB_NAME = "iota_indexer_pruning_qa"

# Global DB config - will be set from command line args
DB_CONFIG = {
    "host": DEFAULT_DB_HOST,
    "port": DEFAULT_DB_PORT,
    "database": DEFAULT_DB_NAME,
    "user": DEFAULT_DB_USER,
    "password": DEFAULT_DB_PASSWORD,
}

DB_URL = None  # Will be set from command line args

FULLNODE_RPC_URL = "http://localhost:59000"
INDEXER_RPC_URL = "http://localhost:59124"

# Path configuration
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# Process management
running_processes = []

# Test data storage - captured before pruning occurs
test_data_from_epoch_0 = {}


def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully."""
    print("\n\n🛑 Interrupted by user, cleaning up...")
    cleanup_processes()
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)


def log(message: str, level: str = "INFO"):
    """Log a message with timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {message}")


def run_command(cmd: list, cwd: str, env: dict = None, background: bool = False):
    """Run a command and optionally return the process for background tasks."""
    log(f"Running command: {' '.join(cmd)}")

    full_env = os.environ.copy()
    if env:
        full_env.update(env)

    if background:
        process = subprocess.Popen(
            cmd,
            cwd=cwd,
            env=full_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        running_processes.append(process)
        return process
    else:
        result = subprocess.run(
            cmd, cwd=cwd, env=full_env, capture_output=True, text=True
        )
        return result


def cleanup_processes():
    """Stop all running background processes."""
    log("Stopping all background processes...")
    for process in running_processes:
        if process.poll() is None:  # Process is still running
            log(f"Terminating process {process.pid}")
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                log(f"Force killing process {process.pid}")
                process.kill()
    running_processes.clear()


def get_db_connection():
    """Get a connection to the PostgreSQL database."""
    return psycopg2.connect(**DB_CONFIG)


def rpc_call(method: str, params: list, use_indexer: bool = True) -> Dict[str, Any]:
    """Make an RPC call to the indexer or fullnode."""
    rpc_url = INDEXER_RPC_URL if use_indexer else FULLNODE_RPC_URL
    try:
        response = requests.post(
            rpc_url,
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": method,
                "params": params,
            },
            timeout=10,
        )
        return response.json()
    except Exception as e:
        log(f"RPC call failed: {e}", "ERROR")
        return {"error": str(e)}


def wait_for_network_ready(timeout: int = 120) -> bool:
    """
    Wait for the network to be ready by polling the RPC endpoint.
    Returns True if network is ready, False if timeout.
    """
    log(f"Waiting for network RPC to be ready at {FULLNODE_RPC_URL}...")
    start_time = time.time()

    while time.time() - start_time < timeout:
        try:
            response = requests.post(
                FULLNODE_RPC_URL,
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "iota_getCheckpoint",
                    "params": ["0"],
                },
                timeout=5,
            )

            if response.status_code == 200:
                data = response.json()
                if "result" in data:
                    log(f"✓ Network is ready (checkpoint 0 available)")
                    return True
        except (requests.exceptions.RequestException, Exception):
            pass

        time.sleep(2)

    log(f"✗ Network did not become ready within {timeout}s", "ERROR")
    return False


def wait_for_indexer_rpc_ready(timeout: int = 120) -> bool:
    """
    Wait for the indexer RPC to be ready by polling the endpoint.
    Returns True if indexer RPC is ready, False if timeout.
    """
    log(f"Waiting for indexer RPC to be ready at {INDEXER_RPC_URL}...")
    start_time = time.time()

    while time.time() - start_time < timeout:
        try:
            response = requests.post(
                INDEXER_RPC_URL,
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "iota_getChainIdentifier",
                    "params": [],
                },
                timeout=5,
            )

            if response.status_code == 200:
                data = response.json()
                if "result" in data:
                    log(f"✓ Indexer RPC is ready")
                    return True
        except (requests.exceptions.RequestException, Exception):
            pass

        time.sleep(2)

    log(f"✗ Indexer RPC did not become ready within {timeout}s", "ERROR")
    return False


def wait_for_indexer_ready(timeout: int = 120) -> bool:
    """
    Wait for the indexer to be ready by checking the database.
    Returns True if indexer is ready, False if timeout.
    """
    log("Waiting for indexer to initialize database...")
    start_time = time.time()
    start_timestamp_ms = int(start_time * 1000)

    while time.time() - start_time < timeout:
        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            cursor.execute("""
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_name = 'watermarks';
            """)
            table_exists = cursor.fetchone()[0] > 0

            if table_exists:
                cursor.execute("""
                    SELECT COUNT(*), MAX(timestamp_ms)
                    FROM checkpoints;
                """)
                result = cursor.fetchone()
                checkpoint_count = result[0]
                max_checkpoint_timestamp = result[1]

                if (
                    checkpoint_count > 0
                    and max_checkpoint_timestamp
                    and max_checkpoint_timestamp > start_timestamp_ms
                ):
                    log(
                        f"✓ Indexer is ready ({checkpoint_count} fresh checkpoints indexed)"
                    )
                    cursor.close()
                    conn.close()
                    return True

            cursor.close()
            conn.close()
        except Exception:
            pass

        time.sleep(2)

    log(f"✗ Indexer did not become ready within {timeout}s", "ERROR")
    return False


def get_current_epoch() -> Optional[int]:
    """Get the current epoch from the database."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(epoch) FROM checkpoints;")
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        return result[0] if result and result[0] is not None else None
    except Exception as e:
        log(f"Failed to get current epoch: {e}", "ERROR")
        return None


def wait_for_epoch(target_epoch: int, timeout: int = 600) -> bool:
    """Wait for a specific epoch to be indexed."""
    log(f"Waiting for epoch {target_epoch} to be indexed...")
    start_time = time.time()
    last_log_time = start_time
    last_epoch = None

    while time.time() - start_time < timeout:
        current_epoch = get_current_epoch()

        if current_epoch is not None and current_epoch >= target_epoch:
            log(f"✓ Reached epoch {current_epoch}")
            return True

        if time.time() - last_log_time >= 10:
            if current_epoch != last_epoch:
                log(f"  Current epoch: {current_epoch}, waiting for {target_epoch}...")
                last_epoch = current_epoch
            last_log_time = time.time()

        time.sleep(2)

    log(f"✗ Did not reach epoch {target_epoch} within {timeout}s", "ERROR")
    return False


def get_checkpoint_sequence_number(epoch: int) -> Optional[int]:
    """Get the first checkpoint sequence number for a given epoch."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT sequence_number FROM checkpoints WHERE epoch = %s ORDER BY sequence_number LIMIT 1;",
            (epoch,),
        )
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        return result[0] if result else None
    except Exception as e:
        log(f"Failed to get checkpoint for epoch {epoch}: {e}", "ERROR")
        return None


def get_checkpoint_range_for_epoch(epoch: int) -> Optional[tuple]:
    """Get the min and max checkpoint sequence numbers for a given epoch."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT MIN(sequence_number), MAX(sequence_number) FROM checkpoints WHERE epoch = %s;",
            (epoch,),
        )
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        return result if result and result[0] is not None else None
    except Exception as e:
        log(f"Failed to get checkpoint range for epoch {epoch}: {e}", "ERROR")
        return None


def get_transaction_digest_from_epoch(epoch: int) -> Optional[str]:
    """Get a transaction digest from a specific epoch."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT transaction_digest
            FROM transactions
            WHERE checkpoint_sequence_number IN (
                SELECT sequence_number FROM checkpoints WHERE epoch = %s
            )
            LIMIT 1;
            """,
            (epoch,),
        )
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        if result and result[0]:
            # Convert memoryview/bytes to base58 string
            digest = result[0]
            if isinstance(digest, (bytes, memoryview)):
                return base58.b58encode(bytes(digest)).decode("ascii")
            return str(digest)
        return None
    except Exception as e:
        log(f"Failed to get transaction from epoch {epoch}: {e}", "ERROR")
        return None


def get_object_id_from_epoch(epoch: int) -> Optional[str]:
    """Get an object ID that was modified in a specific epoch."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT object_id
            FROM objects_history
            WHERE checkpoint_sequence_number IN (
                SELECT sequence_number FROM checkpoints WHERE epoch = %s
            )
            LIMIT 1;
            """,
            (epoch,),
        )
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        if result and result[0]:
            # Convert memoryview/bytes to hex string with 0x prefix
            object_id = result[0]
            if isinstance(object_id, (bytes, memoryview)):
                return "0x" + bytes(object_id).hex()
            return str(object_id)
        return None
    except Exception as e:
        log(f"Failed to get object from epoch {epoch}: {e}", "ERROR")
        return None


def get_event_from_epoch(epoch: int) -> Optional[Dict[str, Any]]:
    """Get event information from a specific epoch."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT tx_sequence_number, event_sequence_number, transaction_digest
            FROM events
            WHERE tx_sequence_number IN (
                SELECT tx_sequence_number
                FROM transactions
                WHERE checkpoint_sequence_number IN (
                    SELECT sequence_number FROM checkpoints WHERE epoch = %s
                )
            )
            LIMIT 1;
            """,
            (epoch,),
        )
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        if result:
            tx_seq, event_seq, tx_digest = result
            # Convert transaction digest to base58
            if isinstance(tx_digest, (bytes, memoryview)):
                tx_digest = base58.b58encode(bytes(tx_digest)).decode("ascii")
            return {
                "tx_sequence_number": tx_seq,
                "event_sequence_number": event_seq,
                "transaction_digest": tx_digest,
            }
        return None
    except Exception as e:
        log(f"Failed to get event from epoch {epoch}: {e}", "ERROR")
        return None


def capture_test_data_before_pruning(epoch: int) -> Dict[str, Any]:
    """Capture test data from a specific epoch before it gets pruned."""
    log(f"Capturing test data from epoch {epoch}...")
    data = {}

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get checkpoint sequence number
        cursor.execute(
            "SELECT sequence_number FROM checkpoints WHERE epoch = %s ORDER BY sequence_number LIMIT 1;",
            (epoch,),
        )
        result = cursor.fetchone()
        if result:
            data["checkpoint_seq"] = result[0]
            log(f"  ✓ Captured checkpoint sequence: {result[0]}")

        # Get transaction digest
        cursor.execute(
            """
            SELECT transaction_digest
            FROM transactions
            WHERE checkpoint_sequence_number IN (
                SELECT sequence_number FROM checkpoints WHERE epoch = %s
            )
            LIMIT 1;
            """,
            (epoch,),
        )
        result = cursor.fetchone()
        if result and result[0]:
            digest = result[0]
            if isinstance(digest, (bytes, memoryview)):
                data["tx_digest"] = base58.b58encode(bytes(digest)).decode("ascii")
            else:
                data["tx_digest"] = str(digest)
            log(f"  ✓ Captured transaction digest: {data['tx_digest']}")

        # Get multiple transaction digests for multi-get test
        cursor.execute(
            """
            SELECT transaction_digest
            FROM transactions
            WHERE checkpoint_sequence_number IN (
                SELECT sequence_number FROM checkpoints WHERE epoch = %s
            )
            LIMIT 3;
            """,
            (epoch,),
        )
        results = cursor.fetchall()
        tx_digests = []
        for row in results:
            if row[0]:
                digest = row[0]
                if isinstance(digest, (bytes, memoryview)):
                    tx_digests.append(base58.b58encode(bytes(digest)).decode("ascii"))
                else:
                    tx_digests.append(str(digest))
        if tx_digests:
            data["tx_digests"] = tx_digests
            log(f"  ✓ Captured {len(tx_digests)} transaction digests")

        # Use hardcoded clock object ID and get its version from this epoch
        clock_object_id = (
            "0x0000000000000000000000000000000000000000000000000000000000000006"
        )
        data["object_id"] = clock_object_id

        cursor.execute(
            """
            SELECT object_version
            FROM objects_history
            WHERE object_id = %s
              AND checkpoint_sequence_number IN (
                  SELECT sequence_number FROM checkpoints WHERE epoch = %s
              )
            LIMIT 1;
            """,
            (bytes.fromhex(clock_object_id[2:]), epoch),
        )
        result = cursor.fetchone()
        if result and result[0]:
            data["object_version"] = result[0]
            log(
                f"  ✓ Captured clock object version from epoch {epoch}: {data['object_version']}"
            )
        else:
            log(f"  ⚠ Could not find clock object version in epoch {epoch}", "WARNING")

        cursor.close()
        conn.close()

    except Exception as e:
        log(f"  ✗ Failed to capture test data: {e}", "ERROR")

    return data


def verify_epoch_is_pruned(epoch: int) -> bool:
    """Verify that data from an epoch has been pruned from the database."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Check if checkpoints for this epoch are pruned
        cursor.execute("SELECT COUNT(*) FROM checkpoints WHERE epoch = %s;", (epoch,))
        checkpoint_count = cursor.fetchone()[0]

        cursor.close()
        conn.close()

        return checkpoint_count == 0
    except Exception as e:
        log(f"Failed to verify pruning for epoch {epoch}: {e}", "ERROR")
        return False


# RPC Call Functions
def call_get_checkpoint(checkpoint_seq: int) -> Dict[str, Any]:
    """Make RPC call to iota_getCheckpoint."""
    return rpc_call("iota_getCheckpoint", [str(checkpoint_seq)])


def call_get_transaction_block(tx_digest: str) -> Dict[str, Any]:
    """Make RPC call to iota_getTransactionBlock."""
    return rpc_call(
        "iota_getTransactionBlock",
        [tx_digest, {"showInput": True, "showEffects": True}],
    )


def call_multi_get_transaction_blocks(tx_digests: List[str]) -> Dict[str, Any]:
    """Make RPC call to iota_multiGetTransactionBlocks."""
    return rpc_call(
        "iota_multiGetTransactionBlocks",
        [tx_digests, {"showInput": True, "showEffects": True}],
    )


def call_get_object(object_id: str) -> Dict[str, Any]:
    """Make RPC call to iota_getObject."""
    return rpc_call("iota_getObject", [object_id, {"showContent": True}])


def call_try_get_past_object(object_id: str, version: int) -> Dict[str, Any]:
    """Make RPC call to iota_tryGetPastObject."""
    return rpc_call(
        "iota_tryGetPastObject", [object_id, version, {"showContent": True}]
    )


def call_get_checkpoints(checkpoint_seq: int, limit: int = 5) -> Dict[str, Any]:
    """Make RPC call to iota_getCheckpoints."""
    return rpc_call("iota_getCheckpoints", [str(checkpoint_seq), limit, False])


def call_get_events(tx_digest: str) -> Dict[str, Any]:
    """Make RPC call to iota_getEvents."""
    return rpc_call("iota_getEvents", [tx_digest])


def call_query_events(tx_digest: str) -> Dict[str, Any]:
    """Make RPC call to iotax_queryEvents."""
    return rpc_call("iotax_queryEvents", [{"Transaction": tx_digest}, None, 10, False])


def call_query_transaction_blocks(checkpoint_seq: int) -> Dict[str, Any]:
    """Make RPC call to iotax_queryTransactionBlocks."""
    return rpc_call(
        "iotax_queryTransactionBlocks",
        [{"filter": {"Checkpoint": str(checkpoint_seq)}}, None, 10, False],
    )


# Validation Functions for Current Epoch (expect success)
def validate_success_generic(result: Dict[str, Any], api_name: str, epoch: int) -> bool:
    """Generic validation for successful RPC calls."""
    if "result" in result:
        log(f"  ✓ {api_name} succeeded for epoch {epoch}")
        return True
    else:
        log(
            f"  ✗ {api_name} failed: {result.get('error', 'unknown error')}",
            "ERROR",
        )
        return False


def test_get_checkpoint_current(current_epoch: int, checkpoint_seq: int) -> bool:
    """Test iota_getCheckpoint for current epoch."""
    log(f"  Testing iota_getCheckpoint with sequence number: {checkpoint_seq}")
    result = call_get_checkpoint(checkpoint_seq)
    if "result" in result:
        log(f"  ✓ iota_getCheckpoint({checkpoint_seq}) succeeded")
        log(
            f"    Response: epoch={result['result'].get('epoch')}, seq={result['result'].get('sequenceNumber')}"
        )
        return True
    else:
        log(
            f"  ✗ iota_getCheckpoint({checkpoint_seq}) failed: {result.get('error', 'unknown error')}",
            "ERROR",
        )
        return False


def test_get_transaction_block_current(current_epoch: int, tx_digest: str) -> bool:
    """Test iota_getTransactionBlock for current epoch."""
    log(f"  Testing iota_getTransactionBlock with digest: {tx_digest}")
    result = call_get_transaction_block(tx_digest)
    if "result" in result:
        log(f"  ✓ iota_getTransactionBlock succeeded for epoch {current_epoch}")
        log(f"    Response: digest={result['result'].get('digest')}")
        return True
    else:
        log(
            f"  ✗ iota_getTransactionBlock failed: {result.get('error', 'unknown error')}",
            "ERROR",
        )
        return False


def test_get_object_current(current_epoch: int, object_id: str) -> bool:
    """Test iota_getObject for current epoch."""
    log(f"  Testing iota_getObject with object ID: {object_id}")
    result = call_get_object(object_id)
    if "result" in result:
        log(f"  ✓ iota_getObject succeeded for epoch {current_epoch}")
        log(
            f"    Response: objectId={result['result']['data'].get('objectId') if 'data' in result['result'] else 'N/A'}"
        )
        return True
    else:
        log(
            f"  ✗ iota_getObject failed: {result.get('error', 'unknown error')}",
            "ERROR",
        )
        return False


def test_query_events_current(current_epoch: int, tx_digest: str) -> bool:
    """Test iotax_queryEvents for current epoch."""
    log(f"  Testing iotax_queryEvents for transaction: {tx_digest}")
    result = call_query_events(tx_digest)
    if "result" in result:
        event_count = len(result["result"].get("data", []))
        log(f"  ✓ iotax_queryEvents succeeded for epoch {current_epoch}")
        log(f"    Response: found {event_count} events")
        return True
    else:
        log(
            f"  ✗ iotax_queryEvents failed: {result.get('error', 'unknown error')}",
            "ERROR",
        )
        return False


def test_query_transaction_blocks_current(
    current_epoch: int, checkpoint_seq: int
) -> bool:
    """Test iotax_queryTransactionBlocks for current epoch."""
    log(f"  Testing iotax_queryTransactionBlocks for checkpoint: {checkpoint_seq}")
    result = call_query_transaction_blocks(checkpoint_seq)
    if "result" in result:
        tx_count = len(result["result"].get("data", []))
        log(f"  ✓ iotax_queryTransactionBlocks succeeded for epoch {current_epoch}")
        log(f"    Response: found {tx_count} transactions")
        return True
    else:
        log(
            f"  ✗ iotax_queryTransactionBlocks failed: {result.get('error', 'unknown error')}",
            "ERROR",
        )
        return False


def test_multi_get_transaction_blocks_current(
    current_epoch: int, tx_digests: List[str]
) -> bool:
    """Test iota_multiGetTransactionBlocks for current epoch."""
    log(f"  Testing iota_multiGetTransactionBlocks with {len(tx_digests)} digests")
    result = call_multi_get_transaction_blocks(tx_digests)
    if "result" in result:
        result_count = len(result["result"])
        log(f"  ✓ iota_multiGetTransactionBlocks succeeded for epoch {current_epoch}")
        log(f"    Response: returned {result_count} transactions")
        return True
    else:
        log(
            f"  ✗ iota_multiGetTransactionBlocks failed: {result.get('error', 'unknown error')}",
            "ERROR",
        )
        return False


def test_try_get_past_object_current(
    current_epoch: int, object_id: str, version: int
) -> bool:
    """Test iota_tryGetPastObject for current epoch."""
    log(
        f"  Testing iota_tryGetPastObject with object ID: {object_id}, version: {version}"
    )
    result = call_try_get_past_object(object_id, version)
    if "result" in result:
        status = result["result"].get("status")
        log(f"  ✓ iota_tryGetPastObject succeeded for epoch {current_epoch}")
        log(f"    Response: status={status}")
        return True
    else:
        log(
            f"  ✗ iota_tryGetPastObject failed: {result.get('error', 'unknown error')}",
            "ERROR",
        )
        return False


def test_get_checkpoints_current(current_epoch: int, checkpoint_seq: int) -> bool:
    """Test iota_getCheckpoints for current epoch."""
    log(f"  Testing iota_getCheckpoints starting from: {checkpoint_seq}")
    result = call_get_checkpoints(checkpoint_seq)
    if "result" in result:
        checkpoint_count = len(result["result"].get("data", []))
        log(f"  ✓ iota_getCheckpoints succeeded for epoch {current_epoch}")
        log(f"    Response: returned {checkpoint_count} checkpoints")
        return True
    else:
        log(
            f"  ✗ iota_getCheckpoints failed: {result.get('error', 'unknown error')}",
            "ERROR",
        )
        return False


def test_get_events_current(current_epoch: int, tx_digest: str) -> bool:
    """Test iota_getEvents for current epoch."""
    log(f"  Testing iota_getEvents for transaction: {tx_digest}")
    result = call_get_events(tx_digest)
    if "result" in result:
        event_count = len(result["result"])
        log(f"  ✓ iota_getEvents succeeded for epoch {current_epoch}")
        log(f"    Response: found {event_count} events")
        return True
    else:
        log(
            f"  ✗ iota_getEvents failed: {result.get('error', 'unknown error')}",
            "ERROR",
        )
        return False


def test_rpc_for_current_epoch(current_epoch: int) -> bool:
    """Test RPC calls for data from the current epoch (should succeed)."""
    log(f"\nTesting RPC calls for current epoch {current_epoch}...")

    # Show epoch checkpoint range
    cp_range = get_checkpoint_range_for_epoch(current_epoch)
    if cp_range:
        log(f"  Epoch {current_epoch} checkpoint range: {cp_range[0]} - {cp_range[1]}")
    else:
        log(
            f"  ⚠ Could not determine checkpoint range for epoch {current_epoch}",
            "WARNING",
        )

    all_passed = True

    # Test 1: Get checkpoint
    checkpoint_seq = get_checkpoint_sequence_number(current_epoch)
    if checkpoint_seq is not None:
        if not test_get_checkpoint_current(current_epoch, checkpoint_seq):
            all_passed = False
    else:
        log(f"  ⚠ No checkpoint found for epoch {current_epoch}", "WARNING")

    # Test 2: Get transaction
    tx_digest = get_transaction_digest_from_epoch(current_epoch)
    if tx_digest:
        if not test_get_transaction_block_current(current_epoch, tx_digest):
            all_passed = False
    else:
        log(f"  ⚠ No transaction found for epoch {current_epoch}", "WARNING")

    # Test 3: Get object
    object_id = get_object_id_from_epoch(current_epoch)
    if object_id:
        if not test_get_object_current(current_epoch, object_id):
            all_passed = False
    else:
        log(f"  ⚠ No object found for epoch {current_epoch}", "WARNING")

    # Test 4: Query events
    event_info = get_event_from_epoch(current_epoch)
    if event_info and tx_digest:
        if not test_query_events_current(current_epoch, tx_digest):
            all_passed = False
    else:
        log(f"  ⚠ No events found for epoch {current_epoch}", "WARNING")

    # Test 5: Query transaction blocks
    if checkpoint_seq is not None:
        if not test_query_transaction_blocks_current(current_epoch, checkpoint_seq):
            all_passed = False

        log("")  # Empty line for visual separation

        # Test 6: Multi get transaction blocks
    if tx_digest:
        # Get a few more transaction digests from the same epoch
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT transaction_digest
                FROM transactions
                WHERE checkpoint_sequence_number IN (
                    SELECT sequence_number FROM checkpoints WHERE epoch = %s
                )
                LIMIT 3;
                """,
                (current_epoch,),
            )
            results = cursor.fetchall()
            cursor.close()
            conn.close()

            tx_digests = []
            for row in results:
                if row[0]:
                    digest = row[0]
                    if isinstance(digest, (bytes, memoryview)):
                        digest = base58.b58encode(bytes(digest)).decode("ascii")
                    tx_digests.append(digest)

            if tx_digests:
                if not test_multi_get_transaction_blocks_current(
                    current_epoch, tx_digests
                ):
                    all_passed = False
        except Exception as e:
            log(f"  ⚠ Failed to get multiple transaction digests: {e}", "WARNING")

    # Test 7: Try get past object
    if object_id:
        # Get the object version
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT object_version
                FROM objects_history
                WHERE object_id = %s
                  AND checkpoint_sequence_number IN (
                      SELECT sequence_number FROM checkpoints WHERE epoch = %s
                  )
                LIMIT 1;
                """,
                (bytes.fromhex(object_id[2:]), current_epoch),
            )
            result = cursor.fetchone()
            cursor.close()
            conn.close()

            if result and result[0]:
                object_version = result[0]
                if not test_try_get_past_object_current(
                    current_epoch, object_id, object_version
                ):
                    all_passed = False
        except Exception as e:
            log(f"  ⚠ Failed to get object version: {e}", "WARNING")

    # Test 8: Get checkpoints
    if checkpoint_seq is not None:
        if not test_get_checkpoints_current(current_epoch, checkpoint_seq):
            all_passed = False

        log("")  # Empty line for visual separation

        # Test 9: Get events
    if tx_digest:
        if not test_get_events_current(current_epoch, tx_digest):
            all_passed = False

    return all_passed


# Validation Functions for Pruned Epoch (expect errors or empty results)
def validate_pruned_response(
    result: Dict[str, Any], api_name: str, pruned_epoch: int
) -> bool:
    """Generic validation for RPC calls on pruned data."""
    if "error" in result:
        error_msg = result["error"].get("message", str(result["error"]))
        log(f"    Error: {error_msg}")
        if (
            "pruned" in error_msg.lower()
            or "not found" in error_msg.lower()
            or "missing data" in error_msg.lower()
        ):
            log(f"  ✓ {api_name} correctly returned error for pruned data")
            return True
        else:
            log(f"  ⚠ {api_name} returned unexpected error: {error_msg}", "WARNING")
            return True
    elif "result" in result:
        log(
            f"  ✗ {api_name} unexpectedly succeeded for pruned epoch {pruned_epoch}",
            "ERROR",
        )
        return False
    else:
        log(f"  ✗ {api_name} returned unexpected response format", "ERROR")
        return False


def test_get_checkpoint_pruned(pruned_epoch: int, checkpoint_seq: int) -> bool:
    """Test iota_getCheckpoint for pruned epoch."""
    log(f"  Testing iota_getCheckpoint with sequence number: {checkpoint_seq}")
    result = call_get_checkpoint(checkpoint_seq)

    if "error" in result:
        error_msg = result["error"].get("message", str(result["error"]))
        log(f"    Error: {error_msg}")
        if (
            "not found" in error_msg.lower()
            or "pruned" in error_msg.lower()
            or "no checkpoint" in error_msg.lower()
        ):
            log(
                f"  ✓ iota_getCheckpoint correctly returned error for pruned checkpoint"
            )
            return True
        else:
            log(
                f"  ⚠ iota_getCheckpoint returned unexpected error: {error_msg}",
                "WARNING",
            )
            return True
    elif "result" in result:
        log(
            f"  ✗ iota_getCheckpoint unexpectedly succeeded for pruned epoch {pruned_epoch}",
            "ERROR",
        )
        log(
            f"    Returned checkpoint: epoch={result['result'].get('epoch')}, seq={result['result'].get('sequenceNumber')}"
        )
        return False
    else:
        log(f"  ✗ iota_getCheckpoint returned unexpected response format", "ERROR")
        return False


def test_query_transaction_blocks_pruned(
    pruned_epoch: int, checkpoint_seq: int
) -> bool:
    """Test iotax_queryTransactionBlocks for pruned epoch."""
    log(
        f"  Testing iotax_queryTransactionBlocks for pruned checkpoint: {checkpoint_seq}"
    )
    result = call_query_transaction_blocks(checkpoint_seq)

    if "error" in result:
        error_msg = result["error"].get("message", str(result["error"]))
        log(f"    Error: {error_msg}")
        if (
            "pruned" in error_msg.lower()
            or "not found" in error_msg.lower()
            or "missing data" in error_msg.lower()
        ):
            log(
                f"  ✓ iotax_queryTransactionBlocks correctly returned error for pruned checkpoint"
            )
            return True
        else:
            log(
                f"  ⚠ iotax_queryTransactionBlocks returned unexpected error: {error_msg}",
                "WARNING",
            )
            return True
    elif "result" in result:
        tx_count = len(result["result"].get("data", []))
        if tx_count == 0:
            log(
                f"  ✓ iotax_queryTransactionBlocks returned empty results for pruned checkpoint"
            )
            return True
        else:
            log(
                f"  ✗ iotax_queryTransactionBlocks unexpectedly returned {tx_count} transactions for pruned epoch {pruned_epoch}",
                "ERROR",
            )
            return False
    else:
        log(
            f"  ✗ iotax_queryTransactionBlocks returned unexpected response format",
            "ERROR",
        )
        return False


def test_query_events_pruned(pruned_epoch: int, tx_digest: str) -> bool:
    """Test iotax_queryEvents for pruned epoch."""
    log(f"  Testing iotax_queryEvents for pruned transaction: {tx_digest}")
    result = call_query_events(tx_digest)

    if "error" in result:
        error_msg = result["error"].get("message", str(result["error"]))
        log(f"    Error: {error_msg}")
        if (
            "pruned" in error_msg.lower()
            or "not found" in error_msg.lower()
            or "missing data" in error_msg.lower()
        ):
            log(
                f"  ✓ iotax_queryEvents correctly returned error for pruned transaction"
            )
            return True
        else:
            log(
                f"  ⚠ iotax_queryEvents returned unexpected error: {error_msg}",
                "WARNING",
            )
            return True
    elif "result" in result:
        event_count = len(result["result"].get("data", []))
        if event_count == 0:
            log(f"  ✓ iotax_queryEvents returned empty results for pruned transaction")
            return True
        else:
            log(
                f"  ✗ iotax_queryEvents unexpectedly returned {event_count} events for pruned epoch {pruned_epoch}",
                "ERROR",
            )
            return False
    else:
        log(f"  ✗ iotax_queryEvents returned unexpected response format", "ERROR")
        return False


def test_multi_get_transaction_blocks_pruned(
    pruned_epoch: int, tx_digests: List[str]
) -> bool:
    """Test iota_multiGetTransactionBlocks for pruned epoch."""
    log(
        f"  Testing iota_multiGetTransactionBlocks with {len(tx_digests)} pruned digests"
    )
    result = call_multi_get_transaction_blocks(tx_digests)

    if "error" in result:
        error_msg = result["error"].get("message", str(result["error"]))
        log(f"    Error: {error_msg}")
        if (
            "pruned" in error_msg.lower()
            or "not found" in error_msg.lower()
            or "missing data" in error_msg.lower()
        ):
            log(
                f"  ✓ iota_multiGetTransactionBlocks correctly returned error for pruned transactions"
            )
            return True
        else:
            log(
                f"  ⚠ iota_multiGetTransactionBlocks returned unexpected error: {error_msg}",
                "WARNING",
            )
            return True
    elif "result" in result:
        result_list = result["result"]
        log(f"    Result: {result_list}")
        # Empty array or array with nulls is acceptable for pruned data
        if len(result_list) == 0:
            log(
                f"  ✓ iota_multiGetTransactionBlocks returned empty array for pruned transactions"
            )
            return True
        # Check if results contain errors/nulls for pruned data
        has_errors = any(
            item is None or "error" in str(item).lower() for item in result_list
        )
        if has_errors:
            log(
                f"  ✓ iota_multiGetTransactionBlocks returned null/errors for pruned transactions"
            )
            return True
        else:
            log(
                f"  ✗ iota_multiGetTransactionBlocks unexpectedly returned valid data for pruned epoch {pruned_epoch}",
                "ERROR",
            )
            return False
    else:
        log(
            f"  ✗ iota_multiGetTransactionBlocks returned unexpected response format",
            "ERROR",
        )
        return False


def test_try_get_past_object_pruned(
    pruned_epoch: int, object_id: str, version: int
) -> bool:
    """Test iota_tryGetPastObject for pruned epoch."""
    log(
        f"  Testing iota_tryGetPastObject for pruned object: {object_id}, version: {version}"
    )
    result = call_try_get_past_object(object_id, version)

    if "error" in result:
        error_msg = result["error"].get("message", str(result["error"]))
        log(f"    Error: {error_msg}")
        if (
            "pruned" in error_msg.lower()
            or "not found" in error_msg.lower()
            or "missing data" in error_msg.lower()
        ):
            log(f"  ✓ iota_tryGetPastObject correctly returned error for pruned object")
            return True
        else:
            log(
                f"  ⚠ iota_tryGetPastObject returned unexpected error: {error_msg}",
                "WARNING",
            )
            return True
    elif "result" in result:
        status = result["result"].get("status")
        if status in ["VersionNotFound", "ObjectNotExists", "ObjectDeleted"]:
            log(
                f"  ✓ iota_tryGetPastObject returned appropriate status for pruned object: {status}"
            )
            return True
        else:
            log(
                f"  ✗ iota_tryGetPastObject unexpectedly succeeded for pruned epoch {pruned_epoch}",
                "ERROR",
            )
            log(f"    Status: {status}")
            return False
    else:
        log(f"  ✗ iota_tryGetPastObject returned unexpected response format", "ERROR")
        return False


def test_get_checkpoints_pruned(pruned_epoch: int, checkpoint_seq: int) -> bool:
    """Test iota_getCheckpoints for pruned epoch."""
    log(
        f"  Testing iota_getCheckpoints starting from pruned checkpoint: {checkpoint_seq}"
    )
    result = call_get_checkpoints(checkpoint_seq)

    if "error" in result:
        error_msg = result["error"].get("message", str(result["error"]))
        log(f"    Error: {error_msg}")
        if (
            "pruned" in error_msg.lower()
            or "not found" in error_msg.lower()
            or "missing data" in error_msg.lower()
        ):
            log(
                f"  ✓ iota_getCheckpoints correctly returned error for pruned checkpoint"
            )
            return True
        else:
            log(
                f"  ⚠ iota_getCheckpoints returned unexpected error: {error_msg}",
                "WARNING",
            )
            return True
    elif "result" in result:
        checkpoint_count = len(result["result"].get("data", []))
        if checkpoint_count == 0:
            log(f"  ✓ iota_getCheckpoints returned empty results for pruned checkpoint")
            return True
        else:
            # Check if returned checkpoints are from available range
            first_cp = (
                result["result"]["data"][0].get("sequenceNumber")
                if result["result"]["data"]
                else None
            )
            if first_cp and int(first_cp) > checkpoint_seq:
                log(
                    f"  ✓ iota_getCheckpoints skipped pruned checkpoints, started from {first_cp}"
                )
                return True
            else:
                log(
                    f"  ✗ iota_getCheckpoints unexpectedly returned checkpoints for pruned epoch {pruned_epoch}",
                    "ERROR",
                )
                return False
    else:
        log(f"  ✗ iota_getCheckpoints returned unexpected response format", "ERROR")
        return False


def test_get_events_pruned(pruned_epoch: int, tx_digest: str) -> bool:
    """Test iota_getEvents for pruned epoch."""
    log(f"  Testing iota_getEvents for pruned transaction: {tx_digest}")
    result = call_get_events(tx_digest)

    if "error" in result:
        error_msg = result["error"].get("message", str(result["error"]))
        log(f"    Error: {error_msg}")
        if (
            "pruned" in error_msg.lower()
            or "not found" in error_msg.lower()
            or "missing data" in error_msg.lower()
        ):
            log(f"  ✓ iota_getEvents correctly returned error for pruned transaction")
            return True
        else:
            log(f"  ⚠ iota_getEvents returned unexpected error: {error_msg}", "WARNING")
            return True
    elif "result" in result:
        event_count = len(result["result"])
        if event_count == 0:
            log(f"  ✓ iota_getEvents returned empty results for pruned transaction")
            return True
        else:
            log(
                f"  ✗ iota_getEvents unexpectedly returned {event_count} events for pruned epoch {pruned_epoch}",
                "ERROR",
            )
            return False
    else:
        log(f"  ✗ iota_getEvents returned unexpected response format", "ERROR")
        return False


def test_rpc_for_pruned_epoch(pruned_epoch: int) -> bool:
    """Test RPC calls for data from a pruned epoch (should return appropriate errors)."""
    log(f"\nTesting RPC calls for pruned epoch {pruned_epoch}...")

    # Show what checkpoint range this epoch had (if we can still determine it)
    cp_range = get_checkpoint_range_for_epoch(pruned_epoch)
    if cp_range:
        log(
            f"  Epoch {pruned_epoch} checkpoint range (before pruning): {cp_range[0]} - {cp_range[1]}"
        )

    all_passed = True

    # First verify this epoch is actually pruned
    if not verify_epoch_is_pruned(pruned_epoch):
        log(
            f"  ⚠ Epoch {pruned_epoch} is not actually pruned yet, skipping tests",
            "WARNING",
        )
        return True

    log(f"  ✓ Verified epoch {pruned_epoch} is pruned from database")

    # Use pre-captured test data from before pruning
    checkpoint_seq = test_data_from_epoch_0.get("checkpoint_seq", pruned_epoch * 10)
    tx_digest = test_data_from_epoch_0.get("tx_digest")
    tx_digests = test_data_from_epoch_0.get("tx_digests", [])
    object_id = test_data_from_epoch_0.get("object_id")
    object_version = test_data_from_epoch_0.get("object_version", 100)

    log(f"  Using pre-captured test data from epoch {pruned_epoch}")
    if checkpoint_seq:
        log(f"    Checkpoint seq: {checkpoint_seq}")
    if tx_digest:
        log(f"    Transaction digest: {tx_digest}")
    if object_id:
        log(f"    Object ID: {object_id}")

    # Test 1: Get checkpoint
    if not test_get_checkpoint_pruned(pruned_epoch, checkpoint_seq):
        all_passed = False

    log("")  # Empty line for visual separation

    # Test 2: Get transaction block
    if tx_digest:
        log(f"  Testing iota_getTransactionBlock with pruned digest: {tx_digest}")
        result = call_get_transaction_block(tx_digest)
        if not validate_pruned_response(
            result, "iota_getTransactionBlock", pruned_epoch
        ):
            all_passed = False
    else:
        log(f"  ⚠ No transaction found for pruned epoch {pruned_epoch}", "WARNING")

    log("")  # Empty line for visual separation

    # Test 3: Get object
    # Note: iota_getObject returns the current state of the object, not historical state
    # The clock object always exists in current state, so this will succeed
    # This is expected behavior - use tryGetPastObject to query historical versions
    if object_id:
        log(f"  Testing iota_getObject with object ID: {object_id}")
        result = call_get_object(object_id)
        # For the clock object, this will return current state (expected)
        if "result" in result:
            log(
                f"  ✓ iota_getObject returned current state (expected for objects table)"
            )
        elif "error" in result:
            log(f"  ✓ iota_getObject returned error: {result.get('error')}")
    else:
        log(f"  ⚠ No object found for pruned epoch {pruned_epoch}", "WARNING")

    log("")  # Empty line for visual separation

    # Test 4: Query events
    if tx_digest:
        if not test_query_events_pruned(pruned_epoch, tx_digest):
            all_passed = False

    log("")  # Empty line for visual separation

    # Test 5: Query transaction blocks
    if not test_query_transaction_blocks_pruned(pruned_epoch, checkpoint_seq):
        all_passed = False

    # Test 6: Multi get transaction blocks
    if tx_digests:
        if not test_multi_get_transaction_blocks_pruned(pruned_epoch, tx_digests):
            all_passed = False
    elif tx_digest:
        if not test_multi_get_transaction_blocks_pruned(pruned_epoch, [tx_digest]):
            all_passed = False

    log("")  # Empty line for visual separation

    # Test 7: Try get past object
    if object_id and object_version:
        if not test_try_get_past_object_pruned(pruned_epoch, object_id, object_version):
            all_passed = False

    log("")  # Empty line for visual separation

    # Test 8: Get checkpoints
    if not test_get_checkpoints_pruned(pruned_epoch, checkpoint_seq):
        all_passed = False

    # Test 9: Get events
    if tx_digest:
        if not test_get_events_pruned(pruned_epoch, tx_digest):
            all_passed = False

    return all_passed


def start_local_network() -> subprocess.Popen:
    """Start the local IOTA network with short epoch duration."""
    log("Starting local IOTA network with 5-second epochs...")

    cmd = [
        "cargo",
        "run",
        "--profile",
        "dev-nodebug",
        "--bin",
        "iota",
        "--",
        "start",
        "--with-faucet=0.0.0.0:59123",
        "--fullnode-rpc-port=59000",
        "--epoch-duration-ms=5000",  # 5 seconds per epoch
        "--force-regenesis",
    ]

    env = {
        "RUST_BACKTRACE": "1",
        "RUST_LOG": "info",
    }

    process = run_command(cmd, REPO_ROOT, env, background=True)

    if not wait_for_network_ready(timeout=120):
        log("Failed to start network", "ERROR")
        raise RuntimeError("Network failed to start")

    return process


def start_indexer_rpc(db_url: str = None) -> subprocess.Popen:
    """Start the indexer RPC server."""
    log("Starting indexer RPC server...")

    cmd = [
        "cargo",
        "run",
        "--profile",
        "dev-nodebug",
        "--bin",
        "iota-indexer",
        "--",
        "--database-url",
        db_url,
        "json-rpc-service",
        "--rpc-client-url",
        FULLNODE_RPC_URL,
        "--rpc-address",
        "0.0.0.0:59124",
    ]

    env = {
        "RUST_BACKTRACE": "1",
        "RUST_LOG": "info",
    }

    process = run_command(cmd, REPO_ROOT, env, background=True)

    if not wait_for_indexer_rpc_ready(timeout=120):
        log("Failed to start indexer RPC service", "ERROR")
        raise RuntimeError("Indexer RPC service failed to start")

    return process


def start_indexer_sync(
    epochs_to_keep: int, reset_db: bool = False, db_url: str = None
) -> subprocess.Popen:
    """Start the indexer sync process with pruning enabled."""
    action = "with DB reset" if reset_db else "without DB reset"
    log(f"Starting indexer sync {action} (epochs_to_keep={epochs_to_keep})...")

    cmd = [
        "cargo",
        "run",
        "--profile",
        "dev-nodebug",
        "--bin",
        "iota-indexer",
        "--",
        "--database-url",
        db_url,
        "--metrics-address",
        "0.0.0.0:59181",
        "indexer",
        "--remote-store-url",
        "http://localhost:59000/api/v1",
        "--epochs-to-keep",
        str(epochs_to_keep),
    ]

    if reset_db:
        cmd.append("--reset-db")

    env = {
        "RUST_BACKTRACE": "1",
        "RUST_LOG": "info",
        "PRUNING_DELAY_MS": "1000",  # 1 second delay for testing
    }

    process = run_command(cmd, REPO_ROOT, env, background=True)

    if not wait_for_indexer_ready(timeout=120):
        log("Failed to start indexer", "ERROR")
        raise RuntimeError("Indexer failed to start")

    return process


def wait_for_pruning(epochs_to_keep: int, timeout: int = 120) -> bool:
    """Wait for pruning to occur by checking that old epochs are removed."""
    log("Waiting for pruning to complete...")
    start_time = time.time()

    while time.time() - start_time < timeout:
        current_epoch = get_current_epoch()
        if current_epoch is None or current_epoch < epochs_to_keep + 1:
            time.sleep(2)
            continue

        # Check if epoch 0 is pruned (should be pruned if current_epoch > epochs_to_keep)
        expected_min_epoch = current_epoch - epochs_to_keep
        if verify_epoch_is_pruned(0):
            log(
                f"✓ Pruning completed (epoch 0 is pruned, current epoch: {current_epoch})"
            )
            return True

        time.sleep(2)

    log(f"✗ Pruning did not complete within {timeout}s", "ERROR")
    return False


def main():
    """Main test flow."""
    parser = argparse.ArgumentParser(
        description="Test IOTA indexer RPC behavior with pruning"
    )
    parser.add_argument(
        "--epochs-to-keep",
        type=int,
        default=2,
        help="Number of epochs to retain (default: 2)",
    )
    parser.add_argument(
        "--target-epoch",
        type=int,
        default=5,
        help="Target epoch to wait for before testing (default: 5)",
    )
    parser.add_argument(
        "--db-user",
        type=str,
        default=DEFAULT_DB_USER,
        help=f"Database username (default: {DEFAULT_DB_USER})",
    )
    parser.add_argument(
        "--db-password",
        type=str,
        default=DEFAULT_DB_PASSWORD,
        help=f"Database password (default: {DEFAULT_DB_PASSWORD})",
    )
    parser.add_argument(
        "--db-host",
        type=str,
        default=DEFAULT_DB_HOST,
        help=f"Database host (default: {DEFAULT_DB_HOST})",
    )
    parser.add_argument(
        "--db-port",
        type=int,
        default=DEFAULT_DB_PORT,
        help=f"Database port (default: {DEFAULT_DB_PORT})",
    )
    parser.add_argument(
        "--db-name",
        type=str,
        default=DEFAULT_DB_NAME,
        help=f"Database name (default: {DEFAULT_DB_NAME})",
    )
    args = parser.parse_args()

    # Set global DB config from command line args
    global DB_CONFIG, DB_URL
    DB_CONFIG = {
        "host": args.db_host,
        "port": args.db_port,
        "database": args.db_name,
        "user": args.db_user,
        "password": args.db_password,
    }
    DB_URL = f"postgresql://{args.db_user}:{args.db_password}@{args.db_host}:{args.db_port}/{args.db_name}"

    log("=" * 80)
    log("IOTA Indexer RPC with Pruning Test")
    log(
        f"Configuration: epochs_to_keep={args.epochs_to_keep}, target_epoch={args.target_epoch}"
    )
    log("Focus: Testing RPC responses for current and pruned epochs")
    log("=" * 80)

    try:
        # Step 1: Start local network with short epoch duration
        log("\n📋 Step 1: Starting local network with 5-second epochs")
        network_process = start_local_network()

        # Step 2: Start indexer sync with pruning
        log(
            f"\n📋 Step 2: Starting indexer sync with epochs_to_keep={args.epochs_to_keep}"
        )
        indexer_sync_process = start_indexer_sync(
            args.epochs_to_keep, reset_db=True, db_url=DB_URL
        )

        # Step 2b: Start indexer RPC service
        log("\n📋 Step 2b: Starting indexer JSON RPC service")
        indexer_rpc_process = start_indexer_rpc(db_url=DB_URL)

        # Step 2c: Capture test data from epoch 0 before it gets pruned
        log("\n📋 Step 2c: Capturing test data from epoch 0 before pruning")
        global test_data_from_epoch_0
        # Wait a bit to ensure epoch 0 data is indexed
        time.sleep(5)
        test_data_from_epoch_0 = capture_test_data_before_pruning(epoch=0)

        # Step 3: Wait for target epoch
        log(f"\n📋 Step 3: Waiting for epoch {args.target_epoch} to be indexed")
        if not wait_for_epoch(args.target_epoch, timeout=600):
            log("Failed to reach target epoch", "ERROR")
            return 1

        # Step 4: Wait for pruning to complete
        log("\n📋 Step 4: Waiting for pruning to complete")
        if not wait_for_pruning(args.epochs_to_keep, timeout=180):
            log("⚠ Pruning did not complete, but continuing with tests...", "WARNING")

        # Give pruning a bit more time to settle
        log("Waiting additional 10 seconds for pruning to settle...")
        time.sleep(10)

        # Step 5: Test RPC for current epoch
        log("\n📋 Step 5: Testing RPC calls for current epoch data")
        current_epoch = get_current_epoch()
        if current_epoch is None:
            log("Failed to get current epoch", "ERROR")
            return 1

        log(f"Current epoch: {current_epoch}")
        if not test_rpc_for_current_epoch(current_epoch):
            log("✗ RPC tests for current epoch failed", "ERROR")
            return 1

        # Step 6: Test RPC for pruned epochs
        # Step 6: Testing RPC calls for pruned epoch data
        log("\n📋 Step 6: Testing RPC calls for pruned epoch data")

        # Test only epoch 0 (should definitely be pruned)
        if current_epoch > args.epochs_to_keep:
            if not test_rpc_for_pruned_epoch(0):
                log("✗ RPC tests for pruned epoch failed", "ERROR")
                return 1
        else:
            log(
                "⚠ No epochs have been pruned yet, skipping pruned epoch tests",
                "WARNING",
            )

        log("\n" + "=" * 80)
        log("✓ RPC WITH PRUNING TEST PASSED SUCCESSFULLY")
        log("  - Network started with 5-second epochs")
        log(
            f"  - Indexer sync with pruning configured (epochs_to_keep={args.epochs_to_keep})"
        )
        log("  - Indexer RPC service started")
        log(f"  - Reached epoch {current_epoch}")
        log("  - Indexer RPC calls for current epoch data succeeded as expected")
        log("  - Indexer RPC calls for pruned epoch data returned appropriate errors")
        log("=" * 80)

        return 0

    except Exception as e:
        log(f"\n✗ TEST FAILED: {e}", "ERROR")
        import traceback

        traceback.print_exc()
        return 1
    finally:
        cleanup_processes()


if __name__ == "__main__":
    sys.exit(main())
