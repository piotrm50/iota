// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2024 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use async_graphql::connection::CursorType;
use iota_indexer::models::objects::StoredHistoryObject;
use serde::{Deserialize, Serialize};

use crate::{
    filter, query,
    raw_query::RawQuery,
    types::cursor::{JsonCursor, Page, ScanLimited},
};

/// The checkpoint sequence number for entities not available for view.
pub(crate) const UNAVAILABLE_CHECKPOINT_SEQUENCE_NUMBER: u64 = u64::MAX;

#[derive(Copy, Clone)]
pub(crate) enum View {
    /// Exact lookup by id+version, no consistency filtering.
    Historical,
    /// Latest state at end of checkpoint.
    Consistent {
        checkpoint_viewed_at: u64,
    },
    /// Latest state at or before `parent_version`, as of the checkpoint where
    /// the parent version was superseded.
    ///
    /// Used for dynamic field queries where the parent object was resolved at
    /// a specific version (e.g. `object(version: 5) { dynamicFields { ... } }`).
    ///
    /// Multiple transactions within the same checkpoint can modify the same
    /// object, producing intra-checkpoint versions. The `parent_version` pins
    /// the view to a specific version within the checkpoint, so that dynamic
    /// fields reflect the state at that parent version rather than at end of
    /// checkpoint.
    ///
    /// `parent_superseded_at` is the checkpoint where the parent version was
    /// itself replaced by a newer version. It is obtained from the backward
    /// history entry used to resolve the parent object. If the parent is still
    /// at the current version (not yet superseded), this falls back to
    /// `checkpoint_viewed_at`.
    ///
    /// Using `parent_superseded_at` instead of `checkpoint_viewed_at` gives a
    /// tighter and more correct scan window — it catches DFs that were
    /// superseded between the parent's creation and `checkpoint_viewed_at`,
    /// which would otherwise be missed.
    ///
    /// The query uses `superseded_at_checkpoint >= parent_superseded_at`
    /// (non-strict `>=`) to include intra-checkpoint versions, and
    /// `object_version <= parent_version` to scope the version. MAX(version)
    /// picks the latest DF version at or before the parent's version.
    ConsistentAtParentVersion {
        parent_superseded_at: u64,
        parent_version: u64,
    },
}

/// The consistent cursor for an index into a `Vec` field is constructed from
/// the index of the element and the checkpoint the cursor was constructed at.
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub(crate) struct ConsistentIndexCursor {
    #[serde(rename = "i")]
    pub ix: usize,
    /// The checkpoint sequence number at which the entity corresponding to this
    /// cursor was viewed at.
    pub c: u64,
}

/// The consistent cursor for an index into a `Map` field is constructed from
/// the name or key of the element and the checkpoint the cursor was constructed
/// at.
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub(crate) struct ConsistentNamedCursor {
    #[serde(rename = "n")]
    pub name: String,
    /// The checkpoint sequence number at which the entity corresponding to this
    /// cursor was viewed at.
    pub c: u64,
}

/// Trait for cursors that have a checkpoint sequence number associated with
/// them.
pub(crate) trait Checkpointed: CursorType {
    fn checkpoint_viewed_at(&self) -> u64;
}

impl Checkpointed for JsonCursor<ConsistentIndexCursor> {
    fn checkpoint_viewed_at(&self) -> u64 {
        self.c
    }
}

impl Checkpointed for JsonCursor<ConsistentNamedCursor> {
    fn checkpoint_viewed_at(&self) -> u64 {
        self.c
    }
}

impl ScanLimited for JsonCursor<ConsistentIndexCursor> {}

impl ScanLimited for JsonCursor<ConsistentNamedCursor> {}

/// Constructs a `RawQuery` using the **backward diff** approach to fetch
/// objects at a consistent point in time.
///
/// # Backward diff approach
///
/// The `objects` table contains the latest state, which may be ahead of
/// `checkpoint_viewed_at`. Instead of building forward from a lagging snapshot,
/// we start from the live `objects` table and apply a backward diff using
/// `objects_backward_history` to undo changes that happened after
/// `checkpoint_viewed_at`.
///
/// The `objects_backward_history` table stores, for each object version change,
/// the **previous** object state and the checkpoint at which that state was
/// superseded (`superseded_at_checkpoint`). An entry with
/// `superseded_at_checkpoint = C` means the row contains the object state that
/// was valid just before checkpoint C replaced it.
///
/// ## Cases handled
///
/// 1. **Object not modified after `checkpoint_viewed_at`** — the row in
///    `objects` is valid. No backward-history entry with
///    `superseded_at_checkpoint > checkpoint_viewed_at` exists, so we return
///    the `objects` row as-is.
///
/// 2. **Object modified after `checkpoint_viewed_at`** — the `objects` row is
///    too new. A backward-history entry exists whose
///    `superseded_at_checkpoint` is the smallest value >
///    `checkpoint_viewed_at`; that entry contains the state valid at
///    `checkpoint_viewed_at`. We return the backward-history row instead.
///
/// 3. **Object created after `checkpoint_viewed_at`** — the object exists in
///    `objects` but didn't exist at `checkpoint_viewed_at`. The backward-history
///    table includes a creation entry for it (with
///    `superseded_at_checkpoint` = creation checkpoint and no previous object
///    data), so the LEFT JOIN exclusion on Source A filters it out. Source B
///    returns the creation entry, but since it has no previous object data, it
///    produces no result row.
///
/// 4. **Object deleted after `checkpoint_viewed_at`** — the object is absent
///    from `objects`. But a backward-history entry with
///    `superseded_at_checkpoint > checkpoint_viewed_at` holds the live state at
///    that point. The backward-history source picks it up.
///
/// 5. **Object modified so it no longer matches the filter** — same as case 2:
///    the backward-history entry may match the filter even though the current
///    `objects` row doesn't.
///
/// 6. **Object modified so it started matching the filter** — the `objects` row
///    matches but the old version doesn't. The backward-history source returns
///    the old (non-matching) version, which the filter rejects. Meanwhile the
///    `objects` source also rejects it because a backward-history entry exists
///    (the object was modified after `checkpoint_viewed_at`). Correctly
///    excluded from both sides.
///
/// ## View modes
///
/// **`View::Historical`** — exact lookup by id+version. No consistency
/// filtering; the LEFT JOIN and backward-history logic are skipped entirely.
///
/// **`View::Consistent`** — end-of-checkpoint state. Source A excludes objects
/// with any backward-history entry where `superseded_at_checkpoint >
/// checkpoint_viewed_at` (strict `>`). Source B picks the MIN(object_version)
/// from entries superseded after `checkpoint_viewed_at` — the pre-modification
/// state.
///
/// **`View::ConsistentAtParentVersion`** — intra-checkpoint version-pinned
/// state. Used for dynamic field queries. Source A excludes objects with any
/// backward-history entry where `superseded_at_checkpoint >=
/// parent_superseded_at` (non-strict `>=`) and `object_version <=
/// parent_version`. Source B picks the MAX(object_version) from entries where
/// `superseded_at_checkpoint >= parent_superseded_at` and
/// `object_version <= parent_version` — the latest version at or before the
/// parent's version. The `>=` (vs `>` in Consistent) opens the window to
/// include intra-checkpoint versions that were superseded within the same
/// checkpoint. Using `parent_superseded_at` (instead of `checkpoint_viewed_at`)
/// ensures DFs superseded between the parent's checkpoint and
/// `checkpoint_viewed_at` are not missed.
///
/// ## Implementation
///
/// **Source A: `objects` table** — objects whose current state is still valid.
/// We LEFT JOIN against `objects_backward_history` to exclude any object that
/// has a backward-history entry indicating it was superseded.
///
/// **Source B: `objects_backward_history`** — previous versions of objects that
/// were superseded. The aggregation function (MIN or MAX) and the boundary
/// condition (`>` or `>=`) depend on the `View` mode.
///
/// The two sources are `UNION ALL`'d and deduplicated with
/// `DISTINCT ON (object_id) ... ORDER BY object_version DESC`.
pub(crate) fn build_objects_query(
    view: View,
    page: &Page<super::types::object::Cursor>,
    filter_fn: impl Fn(RawQuery) -> RawQuery,
) -> RawQuery {
    // --- Source A: live objects from the `objects` table ---
    let mut live_objs_inner = query!("SELECT * FROM objects");
    live_objs_inner = filter_fn(live_objs_inner);

    let mut live_objs = match view {
        View::Consistent { checkpoint_viewed_at } => {
            // Subquery to find older object versions from backward history
            // after checkpoint_viewed_at (strict >). If such an entry exists
            // with a lower version, the `objects` row has been superseded.
            let older = filter!(
                query!("SELECT object_id, object_version FROM objects_backward_history"),
                format!("superseded_at_checkpoint > {}", checkpoint_viewed_at)
            );

            let mut live_objs = query!(
                r#"SELECT candidates.* FROM ({}) candidates
                    LEFT JOIN ({}) older
                    ON (candidates.object_id = older.object_id AND candidates.object_version > older.object_version)"#,
                live_objs_inner,
                older
            );
            live_objs = filter!(live_objs, "older.object_version IS NULL");
            live_objs
        }
        View::ConsistentAtParentVersion { parent_superseded_at, parent_version } => {
            // Non-strict >= to include intra-checkpoint versions, bounded by
            // parent_version to scope the comparison. Uses parent_superseded_at
            // (not checkpoint_viewed_at) to catch DFs superseded between the
            // parent's checkpoint and the current checkpoint.
            let older = filter!(
                query!("SELECT object_id, object_version FROM objects_backward_history"),
                format!(
                    "superseded_at_checkpoint >= {} AND object_version <= {}",
                    parent_superseded_at, parent_version
                )
            );

            let mut live_objs = query!(
                r#"SELECT candidates.* FROM ({}) candidates
                    LEFT JOIN ({}) older
                    ON (candidates.object_id = older.object_id AND candidates.object_version > older.object_version)"#,
                live_objs_inner,
                older
            );
            live_objs = filter!(live_objs, "older.object_version IS NULL");
            live_objs
        }
        View::Historical => {
            query!("SELECT candidates.* FROM ({}) candidates", live_objs_inner)
        }
    };

    live_objs = page.apply::<StoredHistoryObject>(live_objs);

    // --- Source B: previous versions from `objects_backward_history` ---
    let mut history_window = query!("SELECT * FROM objects_backward_history");
    history_window = filter_fn(history_window);

    let mut history_objs = match view {
        View::Consistent { checkpoint_viewed_at } => {
            // Only consider entries superseded after checkpoint_viewed_at
            // (strict >). MIN(object_version) gives the pre-modification state
            // — the version that was live at the end of checkpoint_viewed_at.
            history_window = filter!(
                history_window,
                format!("superseded_at_checkpoint > {}", checkpoint_viewed_at)
            );

            let oldest = filter!(
                query!("SELECT object_id, MIN(object_version) AS min_version FROM objects_backward_history"),
                format!("superseded_at_checkpoint > {}", checkpoint_viewed_at)
            )
            .group_by("object_id");

            query!(
                r#"WITH history_window AS ({}),
                    oldest AS ({})
                    SELECT candidates.* FROM history_window candidates
                    JOIN oldest
                    ON candidates.object_id = oldest.object_id
                    AND candidates.object_version = oldest.min_version"#,
                history_window,
                oldest
            )
        }
        View::ConsistentAtParentVersion { parent_superseded_at, parent_version } => {
            // Non-strict >= using parent_superseded_at to include
            // intra-checkpoint versions and catch DFs superseded between the
            // parent's checkpoint and the current checkpoint. Bounded by
            // parent_version. MAX(object_version) gives the latest version at
            // or before the parent's version.
            history_window = filter!(
                history_window,
                format!(
                    "superseded_at_checkpoint >= {} AND object_version <= {}",
                    parent_superseded_at, parent_version
                )
            );

            let newest = filter!(
                query!("SELECT object_id, MAX(object_version) AS max_version FROM objects_backward_history"),
                format!(
                    "superseded_at_checkpoint >= {} AND object_version <= {}",
                    parent_superseded_at, parent_version
                )
            )
            .group_by("object_id");

            query!(
                r#"WITH history_window AS ({}),
                    newest AS ({})
                    SELECT candidates.* FROM history_window candidates
                    JOIN newest
                    ON candidates.object_id = newest.object_id
                    AND candidates.object_version = newest.max_version"#,
                history_window,
                newest
            )
        }
        View::Historical => {
            query!("SELECT candidates.* FROM ({}) candidates", history_window)
        }
    };

    history_objs = page.apply::<StoredHistoryObject>(history_objs);

    // --- Combine sources ---
    //
    // UNION ALL is safe: for Consistent and ConsistentAtParentVersion views,
    // source A returns objects NOT superseded and source B returns objects that
    // WERE superseded — these sets are disjoint by construction.
    //
    // DISTINCT ON + ORDER BY is a safeguard for the Historical view case
    // and handles any edge cases.
    let query = query!(
        r#"SELECT DISTINCT ON (object_id) * FROM (({}) UNION ALL ({})) candidates"#,
        live_objs,
        history_objs
    )
    .order_by("object_id")
    .order_by("object_version DESC");

    query!("SELECT * FROM ({}) candidates", query)
}
