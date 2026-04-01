// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use std::collections::BTreeMap;

use futures::{StreamExt, stream::ReadyChunks};
use iota_metrics::metered_channel::ReceiverStream;
use tap::tap::TapFallible;
use tracing::{error, info, instrument};

use crate::{
    ingestion::common::{
        persist::{
            CHECKPOINT_COMMIT_BATCH_SIZE, CommitterTables, CommitterWatermark, ThreadBudget,
            WriteTarget,
        },
        prepare::CheckpointObjectChanges,
    },
    metrics::IndexerMetrics,
    models::{
        display::StoredDisplay,
        epoch::{EndOfEpochUpdate, StartOfEpochUpdate},
        obj_indices::StoredObjectVersion,
        transactions::TxGlobalOrder,
    },
    store::{IndexerStore, PgIndexerStore},
    types::{
        EventIndex, IndexedCheckpoint, IndexedDeletedObject, IndexedEvent, IndexedObject,
        IndexedPackage, IndexedTransaction, IndexerResult, TxIndex,
    },
};
#[derive(Debug)]
pub(crate) struct CheckpointDataToCommit {
    pub(crate) checkpoint: IndexedCheckpoint,
    pub(crate) transactions: Vec<IndexedTransaction>,
    pub(crate) events: Vec<IndexedEvent>,
    pub(crate) event_indices: Vec<EventIndex>,
    pub(crate) tx_indices: Vec<TxIndex>,
    pub(crate) display_updates: BTreeMap<String, StoredDisplay>,
    pub(crate) object_changes: CheckpointObjectChanges,
    pub(crate) object_history_changes: TransactionObjectChangesToCommit,
    pub(crate) object_versions: Vec<StoredObjectVersion>,
    pub(crate) packages: Vec<IndexedPackage>,
    pub(crate) epoch: Option<EpochToCommit>,
}

#[derive(Clone, Debug, Default)]
pub struct TransactionObjectChangesToCommit {
    pub changed_objects: Vec<IndexedObject>,
    pub deleted_objects: Vec<IndexedDeletedObject>,
}

#[derive(Clone, Debug)]
pub struct EpochToCommit {
    pub(crate) last_epoch: Option<EndOfEpochUpdate>,
    pub(crate) new_epoch: StartOfEpochUpdate,
}

/// Indexed data for a batch of checkpoints.
struct IndexedCheckpointBatch {
    checkpoints: Vec<IndexedCheckpoint>,
    epoch: Option<EpochToCommit>,
    derived: DerivedData,
}

impl From<Vec<CheckpointDataToCommit>> for IndexedCheckpointBatch {
    fn from(batch: Vec<CheckpointDataToCommit>) -> Self {
        let len = batch.len();
        let mut checkpoints = Vec::with_capacity(len);
        let mut transactions = Vec::with_capacity(len);
        let mut events = Vec::with_capacity(len);
        let mut tx_indices = Vec::with_capacity(len);
        let mut event_indices = Vec::with_capacity(len);
        let mut display_updates = BTreeMap::new();
        let mut object_changes = Vec::with_capacity(len);
        let mut object_history_changes = Vec::with_capacity(len);
        let mut object_versions = Vec::with_capacity(len);
        let mut packages = Vec::with_capacity(len);
        let mut epoch = None;

        for cp in batch {
            checkpoints.push(cp.checkpoint);
            transactions.extend(cp.transactions);
            events.extend(cp.events);
            tx_indices.extend(cp.tx_indices);
            event_indices.extend(cp.event_indices);
            display_updates.extend(cp.display_updates);
            object_changes.push(cp.object_changes);
            object_history_changes.push(cp.object_history_changes);
            object_versions.extend(cp.object_versions);
            packages.extend(cp.packages);
            if cp.epoch.is_some() {
                epoch = cp.epoch;
            }
        }

        let tx_global_order = transactions.iter().map(Into::into).collect();

        Self {
            checkpoints,
            epoch: epoch.clone(),
            derived: DerivedData {
                transactions,
                tx_indices,
                tx_global_order,
                events,
                event_indices,
                display_updates,
                packages,
                object_changes,
                object_history_changes,
                object_versions,
                epoch,
            },
        }
    }
}

/// Derived data from a batch of checkpoints.
///
/// Contains augmented data on transactions, events, objects.
struct DerivedData {
    transactions: Vec<IndexedTransaction>,
    tx_indices: Vec<TxIndex>,
    tx_global_order: Vec<TxGlobalOrder>,
    events: Vec<IndexedEvent>,
    event_indices: Vec<EventIndex>,
    display_updates: BTreeMap<String, StoredDisplay>,
    packages: Vec<IndexedPackage>,
    object_changes: Vec<CheckpointObjectChanges>,
    object_history_changes: Vec<TransactionObjectChangesToCommit>,
    object_versions: Vec<StoredObjectVersion>,
    epoch: Option<EpochToCommit>,
}

pub(crate) struct PrimaryWriter {
    state: PgIndexerStore,
    metrics: IndexerMetrics,
    pub stream: ReadyChunks<ReceiverStream<CheckpointDataToCommit>>,
    pub checkpoint_commit_batch_size: usize,
}

impl PrimaryWriter {
    pub fn new(
        state: PgIndexerStore,
        metrics: IndexerMetrics,
        tx_indexing_receiver: iota_metrics::metered_channel::Receiver<CheckpointDataToCommit>,
    ) -> Self {
        let checkpoint_commit_batch_size = std::env::var("CHECKPOINT_COMMIT_BATCH_SIZE")
            .unwrap_or(CHECKPOINT_COMMIT_BATCH_SIZE.to_string())
            .parse::<usize>()
            .unwrap();
        info!("Using checkpoint commit batch size {checkpoint_commit_batch_size}");

        let stream =
            ReceiverStream::new(tx_indexing_receiver).ready_chunks(checkpoint_commit_batch_size);

        Self {
            state,
            metrics,
            stream,
            checkpoint_commit_batch_size,
        }
    }

    /// Writes indexed checkpoint data to the database, and then update
    /// watermark upper bounds and metrics. Expects
    /// `indexed_checkpoint_batch` to be non-empty, and contain contiguous
    /// checkpoints. There can be at most one epoch boundary at the end. If
    /// an epoch boundary is detected, epoch-partitioned tables must be
    /// advanced.
    // Unwrap: Caller needs to make sure indexed_checkpoint_batch is not empty
    #[instrument(skip_all, fields(
        first = indexed_checkpoint_batch.first().as_ref().unwrap().checkpoint.sequence_number,
        last = indexed_checkpoint_batch.last().as_ref().unwrap().checkpoint.sequence_number
    ))]
    pub(crate) async fn commit_checkpoints(
        &self,
        indexed_checkpoint_batch: Vec<CheckpointDataToCommit>,
    ) {
        let IndexedCheckpointBatch {
            checkpoints: checkpoint_batch,
            epoch,
            derived,
        } = IndexedCheckpointBatch::from(indexed_checkpoint_batch);

        let first_checkpoint_seq = checkpoint_batch.first().unwrap().sequence_number;
        let committer_watermark = CommitterWatermark::from(checkpoint_batch.last().unwrap());
        let checkpoint_num = checkpoint_batch.len();
        let tx_count = derived.transactions.len();

        let guard = self.metrics.checkpoint_db_commit_latency.start_timer();

        self.persist_derived_data(derived)
            .await
            .expect("persisting data into DB should not fail.");

        let is_epoch_end = epoch.is_some();

        // On epoch boundary, we need to modify the existing partitions' upper bound,
        // and introduce a new partition for incoming data for the upcoming epoch.
        if let Some(epoch_data) = epoch {
            self.state
                .advance_epoch(epoch_data)
                .await
                .tap_err(|e| {
                    error!("failed to advance epoch with error: {}", e.to_string());
                })
                .expect("advancing epochs in DB should not fail.");
            self.metrics.total_epoch_committed.inc();

            // Refresh participation metrics after advancing epoch
            self.state
                .refresh_participation_metrics()
                .await
                .tap_err(|e| {
                    error!("failed to update participation metrics: {e}");
                })
                .expect("updating participation metrics should not fail.");
        }

        self.state
            .persist_checkpoints(checkpoint_batch)
            .await
            .tap_err(|e| {
                error!(
                    "failed to persist checkpoint data with error: {}",
                    e.to_string()
                );
            })
            .expect("persisting data into DB should not fail.");

        if is_epoch_end {
            // The epoch has advanced so we update the configs for the new protocol version,
            // if it has changed.
            let chain_id = <PgIndexerStore as IndexerStore>::get_chain_identifier(&self.state)
                .await
                .expect("failed to get chain identifier")
                .expect("chain identifier should have been indexed at this point");
            let _ = self
                .state
                .persist_protocol_configs_and_feature_flags(chain_id);
        }

        self.state
            .update_watermarks_upper_bound::<CommitterTables>(committer_watermark)
            .await
            .tap_err(|e| {
                error!(
                    "Failed to update watermark upper bound with error: {}",
                    e.to_string()
                );
            })
            .expect("Updating watermark upper bound in DB should not fail.");

        let elapsed = guard.stop_and_record();

        info!(
            elapsed,
            "Checkpoint {}-{} committed with {} transactions.",
            first_checkpoint_seq,
            committer_watermark.max_committed_cp,
            tx_count,
        );
        self.metrics
            .latest_tx_checkpoint_sequence_number
            .set(committer_watermark.max_committed_cp as i64);
        self.metrics
            .total_tx_checkpoint_committed
            .inc_by(checkpoint_num as u64);
        self.metrics
            .total_transaction_committed
            .inc_by(tx_count as u64);
        self.metrics.transaction_per_checkpoint.observe(
            tx_count as f64
                / (committer_watermark.max_committed_cp - first_checkpoint_seq + 1) as f64,
        );
        // 1000.0 is not necessarily the batch size, it's to roughly map average tx
        // commit latency to [0.1, 1] seconds, which is well covered by
        // DB_COMMIT_LATENCY_SEC_BUCKETS.
        self.metrics
            .thousand_transaction_avg_db_commit_latency
            .observe(elapsed * 1000.0 / tx_count as f64);
    }

    async fn persist_derived_data(&self, batch: DerivedData) -> IndexerResult<()> {
        let _guard = self
            .metrics
            .checkpoint_db_commit_latency_step_1
            .start_timer();
        let thread_distribution = ThreadBudget::new(self.state.blocking_cp().max_size() as usize)
            .with_tasks(1, WriteTarget::Transactions) // `transactions`
            .with_tasks(10, WriteTarget::Transactions) // `tx_indices`
            .with_tasks(1, WriteTarget::Events) // `events`
            .with_tasks(7, WriteTarget::Events) // `event_indices`
            .with_tasks(3, WriteTarget::Objects) // `objects{,_history,_versions}`
            .build();
        let tx_threads = thread_distribution.threads_per_task(WriteTarget::Transactions);
        let ev_threads = thread_distribution.threads_per_task(WriteTarget::Events);
        let obj_threads = thread_distribution.threads_per_task(WriteTarget::Objects);

        let mut tasks = vec![
            self.state
                .persist_transactions(batch.transactions, tx_threads),
            self.state
                .persist_tx_indices(batch.tx_indices, 10 * tx_threads),
            self.state.persist_events(batch.events, ev_threads),
            self.state
                .persist_event_indices(batch.event_indices, 7 * ev_threads),
            self.state.persist_displays(batch.display_updates),
            self.state.persist_packages(batch.packages),
            self.state
                .persist_object_history(batch.object_history_changes, obj_threads),
            self.state
                .persist_object_versions(batch.object_versions, obj_threads),
            Box::pin(async {
                // We need to persist global order before writing objects, so that optimistic
                // indexing is blocked from overwriting objects table with old tx data
                // reference: https://github.com/iotaledger/iota/issues/10250
                self.state
                    .persist_tx_global_order(batch.tx_global_order, obj_threads)
                    .await?;
                self.state
                    .persist_checkpoint_objects(batch.object_changes, obj_threads)
                    .await
            }),
        ];
        if let Some(epoch_data) = batch.epoch {
            tasks.push(self.state.persist_epoch(epoch_data));
        }

        futures::future::join_all(tasks)
            .await
            .into_iter()
            .map(|res| {
                if res.is_err() {
                    error!("failed to persist data with error: {:?}", res);
                }
                res
            })
            .collect::<IndexerResult<Vec<_>>>()?;
        Ok(())
    }
}
