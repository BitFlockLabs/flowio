use flowio::utils;

#[repr(C)]
struct Node {
    pub link: utils::list::intrusive::slist::Link,
    pub value: u64,
}

const fn node_offset() -> usize {
    std::mem::offset_of!(Node, link)
}

fn node_link_ptr(node: &mut Node) -> *mut utils::list::intrusive::slist::Link {
    node as *mut Node as *mut utils::list::intrusive::slist::Link
}

#[test]
fn test_empty_list() {
    let mut list: utils::list::intrusive::slist::SList<Node> =
        utils::list::intrusive::slist::SList::new();
    assert!(list.is_empty(), "Newly created list should be empty");

    // Popping from an empty list should return None
    assert_eq!(node_offset(), 0);
    let popped = unsafe { list.pop_front() };
    assert!(
        popped.is_none(),
        "Popping an empty list should safely return None"
    );
}

#[test]
fn test_push_pop_lifo() {
    let mut list: utils::list::intrusive::slist::SList<Node> =
        utils::list::intrusive::slist::SList::new_uninit();

    let mut n1 = Node {
        link: utils::list::intrusive::slist::Link::new_unlinked(),
        value: 10,
    };
    let mut n2 = Node {
        link: utils::list::intrusive::slist::Link::new_unlinked(),
        value: 20,
    };
    let mut n3 = Node {
        link: utils::list::intrusive::slist::Link::new_unlinked(),
        value: 30,
    };

    unsafe {
        list.push_front(node_link_ptr(&mut n1));
        list.push_front(node_link_ptr(&mut n2));
        list.push_front(node_link_ptr(&mut n3));
    }

    assert!(
        !list.is_empty(),
        "utils::list::intrusive::slist::SList should not be empty after pushes"
    );

    // Because it's a singly linked list with push_front, it acts as a LIFO stack.
    // Order out: n3 -> n2 -> n1
    unsafe {
        let p3 = list.pop_front().unwrap();
        assert_eq!((*p3).value, 30);
        assert!((*p3).link.is_unlinked(), "pop_front should clear the link");

        let p2 = list.pop_front().unwrap();
        assert_eq!((*p2).value, 20);

        let p1 = list.pop_front().unwrap();
        assert_eq!((*p1).value, 10);
    }

    assert!(
        list.is_empty(),
        "utils::list::intrusive::slist::SList should be empty after popping all elements"
    );
}

#[test]
fn test_push_front_unchecked_edge_case() {
    let mut list: utils::list::intrusive::slist::SList<Node> =
        utils::list::intrusive::slist::SList::new();

    // Simulate a node coming from a pool where the link might still hold garbage
    // or old pointers because we bypass `utils::list::intrusive::slist::Link::new_unlinked()` in the hot loop.
    let mut n1 = Node {
        link: utils::list::intrusive::slist::Link {
            next: 0xdeadbeef as *mut _,
        },
        value: 99,
    };

    unsafe {
        // This should succeed and ignore the dirty link state
        list.push_front_unchecked(node_link_ptr(&mut n1));
    }

    assert!(!list.is_empty());

    unsafe {
        let popped = list.pop_front().unwrap();
        assert_eq!((*popped).value, 99);
        assert!(
            (*popped).link.is_unlinked(),
            "pop_front must clear the link even if it was pushed unchecked"
        );
    }
}

#[test]
fn test_node_reuse() {
    let mut list: utils::list::intrusive::slist::SList<Node> =
        utils::list::intrusive::slist::SList::new();
    let mut n1 = Node {
        link: utils::list::intrusive::slist::Link::new_unlinked(),
        value: 1,
    };

    unsafe {
        list.push_front(node_link_ptr(&mut n1));
        let _ = list.pop_front().unwrap();

        // Because pop_front unlinks the node, we can immediately push it back in without panicking.
        list.push_front(node_link_ptr(&mut n1));
        let popped = list.pop_front().unwrap();
        assert_eq!((*popped).value, 1);
    }
}

#[cfg(all(debug_assertions, not(miri)))]
mod debug_only {
    use super::*;

    #[test]
    #[should_panic(expected = "slist double insert")]
    fn test_double_insert_panics() {
        let mut list: utils::list::intrusive::slist::SList<Node> =
            utils::list::intrusive::slist::SList::new();
        let mut n1 = Node {
            link: utils::list::intrusive::slist::Link::new_unlinked(),
            value: 1,
        };
        let mut n2 = Node {
            link: utils::list::intrusive::slist::Link::new_unlinked(),
            value: 2,
        };

        unsafe {
            list.push_front(node_link_ptr(&mut n1));
            list.push_front(node_link_ptr(&mut n2));
            // n2 is still linked (next -> n1), so double-insert assert fires.
            list.push_front(node_link_ptr(&mut n2));
        }
    }

    #[test]
    #[should_panic(expected = "attempted to push head onto itself")]
    fn test_push_head_onto_itself_panics() {
        let mut list: utils::list::intrusive::slist::SList<Node> =
            utils::list::intrusive::slist::SList::new();
        let mut n1 = Node {
            link: utils::list::intrusive::slist::Link::new_unlinked(),
            value: 1,
        };

        unsafe {
            list.push_front(node_link_ptr(&mut n1));
            // This should panic because n1 is already the head, and push_front_unchecked forbids pushing the head onto itself.
            list.push_front_unchecked(node_link_ptr(&mut n1));
        }
    }

    #[test]
    #[cfg(not(miri))]
    #[should_panic(expected = "singly linked list cycle detected")]
    fn test_cycle_detection_panics() {
        let mut list: utils::list::intrusive::slist::SList<Node> =
            utils::list::intrusive::slist::SList::new();
        let mut n1 = Node {
            link: utils::list::intrusive::slist::Link::new_unlinked(),
            value: 1,
        };
        let mut n2 = Node {
            link: utils::list::intrusive::slist::Link::new_unlinked(),
            value: 2,
        };

        unsafe {
            list.push_front(node_link_ptr(&mut n1));
            list.push_front(node_link_ptr(&mut n2));

            // Manually introduce a cycle: n1 -> n2 -> n1
            n1.link.next = node_link_ptr(&mut n2);
            std::hint::black_box(&n1.link.next);

            // Any list operation that triggers `debug_assert_slist_sanity!` will now traverse and detect the cycle.
            list.pop_front();
        }
    }
}
