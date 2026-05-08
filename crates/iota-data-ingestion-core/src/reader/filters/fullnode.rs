// Copyright (c) 2026 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! Server-side filters for fullnode gRPC connections.
//!
//! Filters tell the fullnode which transactions to include in each
//! checkpoint payload.

use iota_grpc_types::v1::filter as proto;
use iota_sdk_types::{Address, ObjectId, ObjectReference};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransactionKind {
    SystemTransaction,
    ProgrammableTransaction,
    Genesis,
    ConsensusCommitPrologueV1,
    EndOfEpochTransaction,
    RandomnessStateUpdate,
}

impl From<TransactionKind> for proto::TransactionKind {
    fn from(kind: TransactionKind) -> Self {
        match kind {
            TransactionKind::SystemTransaction => proto::TransactionKind::SystemTransaction,
            TransactionKind::ProgrammableTransaction => {
                proto::TransactionKind::ProgrammableTransaction
            }
            TransactionKind::Genesis => proto::TransactionKind::Genesis,
            TransactionKind::ConsensusCommitPrologueV1 => {
                proto::TransactionKind::ConsensusCommitPrologueV1
            }
            TransactionKind::EndOfEpochTransaction => proto::TransactionKind::EndOfEpochTransaction,
            TransactionKind::RandomnessStateUpdate => proto::TransactionKind::RandomnessStateUpdate,
        }
    }
}

/// Filter applied to transactions in a fullnode checkpoint stream.
///
/// Each leaf filter is constructed via a named factory method. Combine
/// leaves with [`TransactionFilter::all`], [`TransactionFilter::any`] and
/// [`TransactionFilter::negation`] for boolean logic.
///
/// # Example
///
/// ```rust
/// use iota_data_ingestion_core::filters::fullnode::{TransactionFilter, TransactionKind};
///
/// let filter = TransactionFilter::all([
///     TransactionFilter::kinds([TransactionKind::ProgrammableTransaction]),
///     TransactionFilter::execution_status(true),
/// ]);
/// ```
#[derive(Clone, Debug)]
pub struct TransactionFilter(proto::TransactionFilter);

impl TransactionFilter {
    /// Matches transactions of any of the given [`TransactionKind`]s.
    pub fn kinds(kinds: impl IntoIterator<Item = TransactionKind>) -> Self {
        let transaction_kinds_filter =
            kinds
                .into_iter()
                .fold(proto::TransactionKindsFilter::default(), |mut acc, kind| {
                    acc.push_kinds(kind.into());
                    acc
                });

        Self(proto::TransactionFilter::default().with_transaction_kinds(transaction_kinds_filter))
    }

    /// Matches transactions by execution status.
    ///
    /// - `true` for successful transactions.
    /// - `false` for failed transactions.
    pub fn execution_status(success: bool) -> Self {
        Self(
            proto::TransactionFilter::default().with_execution_status(
                proto::ExecutionStatusFilter::default().with_success(success),
            ),
        )
    }

    /// Matches transactions sent by the given address.
    pub fn sender(address: Address) -> Self {
        Self(
            proto::TransactionFilter::default()
                .with_sender(proto::AddressFilter::default().with_address(address)),
        )
    }

    /// Matches transactions whose recipient is the given address.
    pub fn receiver(address: Address) -> Self {
        Self(
            proto::TransactionFilter::default()
                .with_receiver(proto::AddressFilter::default().with_address(address)),
        )
    }

    /// Matches transactions that touch the given object.
    pub fn affected_object(object_ref: ObjectReference) -> Self {
        Self(
            proto::TransactionFilter::default()
                .with_affected_object(proto::ObjectIdFilter::default().with_object_ref(object_ref)),
        )
    }

    /// Matches transactions containing a command that satisfies the given
    /// [`CommandFilter`].
    pub fn command(filter: CommandFilter) -> Self {
        Self(proto::TransactionFilter::default().with_command(filter))
    }

    /// Matches transactions that contain at least one event satisfying the
    /// given [`EventFilter`].
    pub fn event(filter: EventFilter) -> Self {
        Self(proto::TransactionFilter::default().with_event(filter))
    }

    /// Logical `AND` of the given sub-filters.
    pub fn all(filters: impl IntoIterator<Item = TransactionFilter>) -> Self {
        let filters = filters.into_iter().map(Into::into).collect();
        Self(
            proto::TransactionFilter::default()
                .with_all(proto::AllTransactionFilter::default().with_filters(filters)),
        )
    }

    /// Logical `OR` of the given sub-filters.
    pub fn any(filters: impl IntoIterator<Item = TransactionFilter>) -> Self {
        let filters = filters.into_iter().map(Into::into).collect();
        Self(
            proto::TransactionFilter::default()
                .with_any(proto::AnyTransactionFilter::default().with_filters(filters)),
        )
    }

    /// Logical `NOT` of this filter.
    pub fn negation(self) -> Self {
        Self(
            proto::TransactionFilter::default()
                .with_negation(proto::NotTransactionFilter::default().with_filter(self.0)),
        )
    }
}

impl From<TransactionFilter> for proto::TransactionFilter {
    fn from(value: TransactionFilter) -> Self {
        value.0
    }
}

/// Filter for commands within a programmable transaction.
///
/// Used as input to [`TransactionFilter::command`].
#[derive(Clone, Debug)]
pub struct CommandFilter(proto::CommandFilter);

impl CommandFilter {
    /// Matches any `MoveCall` to the given package.
    pub fn move_call(package_id: ObjectId) -> Self {
        Self(
            proto::CommandFilter::default().with_move_call(
                proto::MoveCallCommandFilter::default().with_package_id(package_id),
            ),
        )
    }

    /// Matches any `MoveCall` to the given package and module.
    pub fn move_call_in_module(package_id: ObjectId, module: impl Into<String>) -> Self {
        Self(
            proto::CommandFilter::default().with_move_call(
                proto::MoveCallCommandFilter::default()
                    .with_package_id(package_id)
                    .with_module(module),
            ),
        )
    }

    /// Matches a specific `MoveCall` to the given package, module and
    /// function.
    pub fn move_call_to(
        package_id: ObjectId,
        module: impl Into<String>,
        function: impl Into<String>,
    ) -> Self {
        Self(
            proto::CommandFilter::default().with_move_call(
                proto::MoveCallCommandFilter::default()
                    .with_package_id(package_id)
                    .with_module(module)
                    .with_function(function),
            ),
        )
    }

    /// Matches any `TransferObjects` command.
    pub fn transfer_objects() -> Self {
        Self(
            proto::CommandFilter::default()
                .with_transfer_objects(proto::TransferObjectsCommandFilter::default()),
        )
    }

    /// Matches any `SplitCoins` command.
    pub fn split_coins() -> Self {
        Self(
            proto::CommandFilter::default()
                .with_split_coins(proto::SplitCoinsCommandFilter::default()),
        )
    }

    /// Matches any `MergeCoins` command.
    pub fn merge_coins() -> Self {
        Self(
            proto::CommandFilter::default()
                .with_merge_coins(proto::MergeCoinsCommandFilter::default()),
        )
    }

    /// Matches any `Publish` command.
    pub fn publish() -> Self {
        Self(proto::CommandFilter::default().with_publish(proto::PublishCommandFilter::default()))
    }

    /// Matches any `MakeMoveVec` command.
    pub fn make_move_vec() -> Self {
        Self(
            proto::CommandFilter::default()
                .with_make_move_vec(proto::MakeMoveVecCommandFilter::default()),
        )
    }

    /// Matches any `Upgrade` command.
    pub fn upgrade() -> Self {
        Self(proto::CommandFilter::default().with_upgrade(proto::UpgradeCommandFilter::default()))
    }

    /// Matches an `Upgrade` command for the given package.
    pub fn upgrade_of(package_id: ObjectId) -> Self {
        Self(
            proto::CommandFilter::default()
                .with_upgrade(proto::UpgradeCommandFilter::default().with_package_id(package_id)),
        )
    }
}

impl From<CommandFilter> for proto::CommandFilter {
    fn from(value: CommandFilter) -> Self {
        value.0
    }
}

/// Filter for events emitted by transactions.
///
/// Used as input to [`TransactionFilter::event`] to match transactions that
/// contain events satisfying this filter.
#[derive(Clone, Debug)]
pub struct EventFilter(proto::EventFilter);

impl EventFilter {
    /// Matches events whose enclosing transaction was sent by the given
    /// address.
    pub fn sender(address: Address) -> Self {
        Self(
            proto::EventFilter::default()
                .with_sender(proto::AddressFilter::default().with_address(address)),
        )
    }

    /// Matches events emitted by a transaction whose top-level `MoveCall`
    /// targets the given package.
    ///
    /// This matches the package the event was *emitted from*,
    /// not where the event struct is defined. For the latter, use
    /// [`EventFilter::defined_in`] / [`EventFilter::defined_in_module`].
    pub fn emitted_in(package_id: ObjectId) -> Self {
        Self(proto::EventFilter::default().with_move_package_and_module(
            proto::MovePackageAndModuleFilter::default().with_package_id(package_id),
        ))
    }

    /// Matches events emitted by a transaction whose top-level `MoveCall`
    /// targets the given package and module.
    ///
    /// This matches the package and module the event was *emitted from*,
    /// not where the event struct is defined. For the latter, use
    /// [`EventFilter::defined_in`] / [`EventFilter::defined_in_module`].
    pub fn emitted_in_module(package_id: ObjectId, module: impl Into<String>) -> Self {
        Self(
            proto::EventFilter::default().with_move_package_and_module(
                proto::MovePackageAndModuleFilter::default()
                    .with_package_id(package_id)
                    .with_module(module),
            ),
        )
    }

    /// Matches events whose struct is defined in the given package.
    ///
    /// This matches the package the event struct is *defined
    /// in*, not where it was emitted from. For the latter, use
    /// [`EventFilter::emitted_in`] / [`EventFilter::emitted_in_module`].
    pub fn defined_in(package_id: ObjectId) -> Self {
        Self(
            proto::EventFilter::default().with_move_event_package_and_module(
                proto::MovePackageAndModuleFilter::default().with_package_id(package_id),
            ),
        )
    }

    /// Matches events whose struct is defined in the given package and module.
    ///
    /// This matches the package and module the event struct is *defined
    /// in*, not where it was emitted from. For the latter, use
    /// [`EventFilter::emitted_in`] / [`EventFilter::emitted_in_module`].
    pub fn defined_in_module(package_id: ObjectId, module: impl Into<String>) -> Self {
        Self(
            proto::EventFilter::default().with_move_event_package_and_module(
                proto::MovePackageAndModuleFilter::default()
                    .with_package_id(package_id)
                    .with_module(module),
            ),
        )
    }

    /// Matches events with the given Move event struct tag (e.g.
    /// `"0xabcd::my_module::Foo"`).
    pub fn event_type(struct_tag: impl Into<String>) -> Self {
        Self(proto::EventFilter::default().with_move_event_type(
            proto::MoveEventTypeFilter::default().with_struct_tag(struct_tag),
        ))
    }

    /// Logical `AND` of the given sub-filters.
    pub fn all(filters: impl IntoIterator<Item = EventFilter>) -> Self {
        let filters = filters.into_iter().map(Into::into).collect();
        Self(
            proto::EventFilter::default()
                .with_all(proto::AllEventFilter::default().with_filters(filters)),
        )
    }

    /// Logical `OR` of the given sub-filters.
    pub fn any(filters: impl IntoIterator<Item = EventFilter>) -> Self {
        let filters = filters.into_iter().map(Into::into).collect();
        Self(
            proto::EventFilter::default()
                .with_any(proto::AnyEventFilter::default().with_filters(filters)),
        )
    }

    /// Logical `NOT` of this filter.
    pub fn negation(self) -> Self {
        Self(
            proto::EventFilter::default()
                .with_negation(proto::NotEventFilter::default().with_filter(self.0)),
        )
    }
}

impl From<EventFilter> for proto::EventFilter {
    fn from(value: EventFilter) -> Self {
        value.0
    }
}

#[cfg(test)]
mod tests {
    use crate::reader::filters::fullnode::*;

    #[test]
    fn complex_nested_composition_matches_proto() {
        let pkg = ObjectId::ZERO;
        let alice = Address::ZERO;
        let bob = Address::ZERO;

        // (Programmable AND success AND command(MoveCall pkg::events))
        // OR (sender(alice) AND event(event_type("0x1::events::Foo") OR
        // emitted_in_module(pkg, "events"))) OR NOT(receiver(bob))
        let wrapper = TransactionFilter::any([
            TransactionFilter::all([
                TransactionFilter::kinds([TransactionKind::ProgrammableTransaction]),
                TransactionFilter::execution_status(true),
                TransactionFilter::command(CommandFilter::move_call_in_module(pkg, "events")),
            ]),
            TransactionFilter::all([
                TransactionFilter::sender(alice),
                TransactionFilter::event(EventFilter::any([
                    EventFilter::event_type("0x1::events::Foo"),
                    EventFilter::emitted_in_module(pkg, "events"),
                ])),
            ]),
            TransactionFilter::receiver(bob).negation(),
        ]);

        let expected = proto::TransactionFilter::default().with_any(
            proto::AnyTransactionFilter::default().with_filters(vec![
                // Branch 1: ALL [kinds, success, command]
                proto::TransactionFilter::default().with_all(
                    proto::AllTransactionFilter::default().with_filters(vec![
                        proto::TransactionFilter::default().with_transaction_kinds({
                            let mut k = proto::TransactionKindsFilter::default();
                            k.push_kinds(TransactionKind::ProgrammableTransaction.into());
                            k
                        }),
                        proto::TransactionFilter::default().with_execution_status(
                            proto::ExecutionStatusFilter::default().with_success(true),
                        ),
                        proto::TransactionFilter::default().with_command(
                            proto::CommandFilter::default().with_move_call(
                                proto::MoveCallCommandFilter::default()
                                    .with_package_id(pkg)
                                    .with_module("events"),
                            ),
                        ),
                    ]),
                ),
                // Branch 2: ALL [sender, event(any[event_type, emitted_in_module])]
                proto::TransactionFilter::default().with_all(
                    proto::AllTransactionFilter::default().with_filters(vec![
                        proto::TransactionFilter::default()
                            .with_sender(proto::AddressFilter::default().with_address(alice)),
                        proto::TransactionFilter::default().with_event(
                            proto::EventFilter::default().with_any(
                                proto::AnyEventFilter::default().with_filters(vec![
                                    proto::EventFilter::default().with_move_event_type(
                                        proto::MoveEventTypeFilter::default()
                                            .with_struct_tag("0x1::events::Foo"),
                                    ),
                                    proto::EventFilter::default().with_move_package_and_module(
                                        proto::MovePackageAndModuleFilter::default()
                                            .with_package_id(pkg)
                                            .with_module("events"),
                                    ),
                                ]),
                            ),
                        ),
                    ]),
                ),
                // Branch 3: NOT receiver(bob)
                proto::TransactionFilter::default().with_negation(
                    proto::NotTransactionFilter::default().with_filter(
                        proto::TransactionFilter::default()
                            .with_receiver(proto::AddressFilter::default().with_address(bob)),
                    ),
                ),
            ]),
        );

        assert_eq!(proto::TransactionFilter::from(wrapper), expected);
    }
}
