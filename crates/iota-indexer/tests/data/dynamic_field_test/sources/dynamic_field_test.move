// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

module dynamic_field_test::dynamic_field_test {
    use iota::dynamic_field as field;
    use iota::dynamic_object_field as ofield;
    use iota::object;
    use iota::tx_context::TxContext;
    use iota::transfer;
    use std::string::{String, utf8};

    struct Parent has key, store {
        id: object::UID,
        value: u64,
    }

    /// Child object that can be used as a dynamic object field.
    /// It has its own UID and can itself hold dynamic fields.
    struct Child has key, store {
        id: object::UID,
        count: u64,
    }

    // ---------------------------------------------------------------
    // Original test functions (flat DFs on Parent)
    // ---------------------------------------------------------------

    /// Creates a Parent object with two dynamic fields attached:
    /// "counter" (u64) and "label" (String).
    public entry fun create_parent_with_df(recipient: address, ctx: &mut TxContext) {
        let parent = Parent {
            id: object::new(ctx),
            value: 42,
        };
        field::add<String, u64>(&mut parent.id, utf8(b"counter"), 0);
        field::add<String, String>(&mut parent.id, utf8(b"label"), utf8(b"hello"));
        transfer::public_transfer(parent, recipient);
    }

    /// Read-only access to the parent - reads only the "counter" DF.
    public entry fun read_parent(parent: &Parent) {
        let _val = parent.value;
        let _df_val = *field::borrow<String, u64>(&parent.id, utf8(b"counter"));
    }

    /// Mutates only the "counter" dynamic field on the parent.
    public entry fun mutate_df(parent: &mut Parent) {
        let counter = field::borrow_mut<String, u64>(&mut parent.id, utf8(b"counter"));
        *counter = *counter + 1;
    }

    // ---------------------------------------------------------------
    // Chained dynamic object field tests: Parent -> DOF1 -> DOF2
    // ---------------------------------------------------------------

    /// Creates a Parent with a chained structure:
    /// Parent --(ofield "child")--> Child (DOF1) --(ofield "grandchild")--> Child (DOF2)
    public entry fun create_chain(recipient: address, ctx: &mut TxContext) {
        let grandchild = Child {
            id: object::new(ctx),
            count: 0,
        };
        let child = Child {
            id: object::new(ctx),
            count: 0,
        };
        ofield::add(&mut child.id, utf8(b"grandchild"), grandchild);
        let parent = Parent {
            id: object::new(ctx),
            value: 100,
        };
        ofield::add(&mut parent.id, utf8(b"child"), child);
        transfer::public_transfer(parent, recipient);
    }

    /// Read DOF1 (child) via the parent — does NOT modify anything.
    /// Parent is passed as &mut because ofield::borrow requires &UID
    /// but the child is only read.
    public entry fun read_child(parent: &Parent) {
        let child: &Child = ofield::borrow(&parent.id, utf8(b"child"));
        let _val = child.count;
    }

    /// Read DOF2 (grandchild) through the chain: Parent -> DOF1 -> DOF2.
    /// Only reads, no mutation. DOF1 is traversed (read) to reach DOF2.
    public entry fun read_grandchild(parent: &Parent) {
        let child: &Child = ofield::borrow(&parent.id, utf8(b"child"));
        let grandchild: &Child = ofield::borrow(&child.id, utf8(b"grandchild"));
        let _val = grandchild.count;
    }

    /// Mutate only DOF2 (grandchild) through the chain.
    /// Parent and DOF1 are passed through mutably to reach DOF2.
    public entry fun mutate_grandchild(parent: &mut Parent) {
        let child: &mut Child = ofield::borrow_mut(&mut parent.id, utf8(b"child"));
        let grandchild: &mut Child = ofield::borrow_mut(&mut child.id, utf8(b"grandchild"));
        grandchild.count = grandchild.count + 1;
    }

    /// Mutate only DOF1 (child), leave DOF2 (grandchild) untouched.
    public entry fun mutate_child(parent: &mut Parent) {
        let child: &mut Child = ofield::borrow_mut(&mut parent.id, utf8(b"child"));
        child.count = child.count + 1;
    }

}
