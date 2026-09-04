use flowio::test_support::utils;
use std::mem::offset_of;
use std::ptr;

#[repr(C)]
pub struct Task {
    pub id: u32,
    pub link: utils::list::intrusive::dlist::Link,
}

// Derive the link pointer from the container base, not `&mut task.link`, so it
// keeps whole-`Task` provenance. DList recovers the container with
// `(link_ptr as *mut u8).sub(offset)`, which is UB under Miri if the pointer
// was narrowed to just the `link` field.
fn task_link_ptr(task: &mut Task) -> *mut utils::list::intrusive::dlist::Link {
    let base = task as *mut Task as *mut u8;
    unsafe { base.add(offset_of!(Task, link)) as *mut utils::list::intrusive::dlist::Link }
}

#[test]
fn remove_unlinked_node_is_noop() {
    let mut list = utils::list::intrusive::dlist::DList::<Task>::new_uninit();
    list.init();
    let mut task = Task {
        id: 1,
        link: utils::list::intrusive::dlist::Link::new_unlinked(),
    };

    unsafe {
        list.remove(task_link_ptr(&mut task));
    }

    assert!(list.is_empty());
}

#[test]
#[cfg(debug_assertions)]
fn remove_cross_list_node_panics_in_debug() {
    let mut left = utils::list::intrusive::dlist::DList::<Task>::new_uninit();
    left.init();
    let mut right = utils::list::intrusive::dlist::DList::<Task>::new_uninit();
    right.init();
    let mut task = Task {
        id: 1,
        link: utils::list::intrusive::dlist::Link::new_unlinked(),
    };

    unsafe {
        right.push_back(task_link_ptr(&mut task));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            left.remove(task_link_ptr(&mut task));
        }));
        assert!(result.is_err());
        while right.pop_front(offset_of!(Task, link)).is_some() {}
    }
}

#[test]
#[cfg(debug_assertions)]
fn test_broken_list_detection() {
    let mut list = utils::list::intrusive::dlist::DList::<Task>::new_uninit();
    list.init();

    let mut t1 = Task {
        id: 1,
        link: utils::list::intrusive::dlist::Link::new_unlinked(),
    };
    let mut t2 = Task {
        id: 2,
        link: utils::list::intrusive::dlist::Link::new_unlinked(),
    };

    unsafe {
        list.push_back(task_link_ptr(&mut t1));

        // Corrupt t1.next so the sentinel's prev.next no longer points back
        // to head.
        t1.link.next = std::ptr::null_mut();
        std::hint::black_box(&t1.link.next);

        // The next push_back runs the debug sentinel consistency check, which
        // must detect `prev.next != head`.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            list.push_back(task_link_ptr(&mut t2));
        }));
        assert!(result.is_err());
        // The list is intentionally corrupted and non-empty; suppress Drop's
        // debug assertion after the caught panic.
        std::mem::forget(list);
    }
}

#[repr(C)]
struct Node {
    value: u32,
    link: utils::list::intrusive::dlist::Link,
}

fn new_node(value: u32) -> Node {
    Node {
        value,
        link: utils::list::intrusive::dlist::Link::new_unlinked(),
    }
}

fn link_offset() -> usize {
    offset_of!(Node, link)
}

// Offset-based for the same provenance reason as `task_link_ptr`: the link
// pointer must keep whole-`Node` provenance so DList's `.sub(offset)` container
// recovery stays sound under Miri.
fn link_ptr(n: &mut Node) -> *mut utils::list::intrusive::dlist::Link {
    let base = n as *mut Node as *mut u8;
    unsafe { base.add(offset_of!(Node, link)) as *mut utils::list::intrusive::dlist::Link }
}

#[test]
fn empty_uninit_then_init() {
    let off = link_offset();

    let mut list: utils::list::intrusive::dlist::DList<Node> =
        utils::list::intrusive::dlist::DList::new_uninit();
    assert!(list.is_empty());

    list.init();
    assert!(list.is_empty());
    assert!(unsafe { list.front(off).is_none() });
}

#[test]
fn front_peek_does_not_remove() {
    let off = link_offset();

    let mut list: utils::list::intrusive::dlist::DList<Node> =
        utils::list::intrusive::dlist::DList::new_uninit();
    list.init();

    let mut n1 = new_node(111);
    let p1 = link_ptr(&mut n1);
    unsafe {
        list.push_back(p1);
    }

    // front() should not remove
    let f1 = unsafe { list.front(off).unwrap() };
    assert_eq!(unsafe { (*f1).value }, 111);
    assert!(!list.is_empty());

    let f2 = unsafe { list.front(off).unwrap() };
    assert_eq!(unsafe { (*f2).value }, 111);
    assert!(!list.is_empty());

    let popped = unsafe { list.pop_front(off).unwrap() };
    assert_eq!(unsafe { (*popped).value }, 111);
    assert!(list.is_empty());
    assert!(n1.link.is_unlinked());
}

#[test]
fn push_back_order_then_pop_front() {
    let off = link_offset();

    let mut list: utils::list::intrusive::dlist::DList<Node> =
        utils::list::intrusive::dlist::DList::new_uninit();
    list.init();

    let mut n1 = new_node(1);
    let mut n2 = new_node(2);
    let mut n3 = new_node(3);

    unsafe {
        let p1 = link_ptr(&mut n1);
        list.push_back(p1);

        let p2 = link_ptr(&mut n2);
        list.push_back(p2);

        let p3 = link_ptr(&mut n3);
        list.push_back(p3);

        let a = list.pop_front(off).unwrap();
        assert_eq!((*a).value, 1);

        let b = list.pop_front(off).unwrap();
        assert_eq!((*b).value, 2);

        let c = list.pop_front(off).unwrap();
        assert_eq!((*c).value, 3);

        assert!(list.pop_front(off).is_none());
        assert!(list.is_empty());
    }

    assert!(n1.link.is_unlinked());
    assert!(n2.link.is_unlinked());
    assert!(n3.link.is_unlinked());
}

#[test]
fn push_front_order_then_pop_front() {
    let off = link_offset();

    let mut list: utils::list::intrusive::dlist::DList<Node> =
        utils::list::intrusive::dlist::DList::new_uninit();
    list.init();

    let mut n1 = new_node(1);
    let mut n2 = new_node(2);
    let mut n3 = new_node(3);

    unsafe {
        list.push_front(link_ptr(&mut n1)); // [1]

        list.push_front(link_ptr(&mut n2)); // [2,1]

        list.push_front(link_ptr(&mut n3)); // [3,2,1]

        let a = list.pop_front(off).unwrap();
        let b = list.pop_front(off).unwrap();
        let c = list.pop_front(off).unwrap();

        assert_eq!((*a).value, 3);
        assert_eq!((*b).value, 2);
        assert_eq!((*c).value, 1);
        assert!(list.is_empty());
    }
}

#[test]
fn mixed_pushes_expected_order() {
    let off = link_offset();

    let mut list: utils::list::intrusive::dlist::DList<Node> =
        utils::list::intrusive::dlist::DList::new_uninit();
    list.init();

    let mut n1 = new_node(1);
    let mut n2 = new_node(2);
    let mut n3 = new_node(3);

    unsafe {
        list.push_back(link_ptr(&mut n1)); // [1]
        list.push_back(link_ptr(&mut n2)); // [1,2]
        list.push_front(link_ptr(&mut n3)); // [3,1,2]

        let a = list.pop_front(off).unwrap();
        let b = list.pop_front(off).unwrap();
        let c = list.pop_front(off).unwrap();

        assert_eq!((*a).value, 3);
        assert_eq!((*b).value, 1);
        assert_eq!((*c).value, 2);
        assert!(list.pop_front(off).is_none());
        assert!(list.is_empty());
    }
}

#[test]
fn remove_arbitrary_middle() {
    let off = link_offset();

    let mut list: utils::list::intrusive::dlist::DList<Node> =
        utils::list::intrusive::dlist::DList::new_uninit();
    list.init();

    let mut n1 = new_node(10);
    let mut n2 = new_node(20);
    let mut n3 = new_node(30);

    unsafe {
        list.push_back(link_ptr(&mut n1));
        list.push_back(link_ptr(&mut n2));
        list.push_back(link_ptr(&mut n3));

        let p2 = link_ptr(&mut n2);
        list.remove(p2);

        assert!(n2.link.is_unlinked());

        let a = list.pop_front(off).unwrap();
        let b = list.pop_front(off).unwrap();

        assert_eq!((*a).value, 10);
        assert_eq!((*b).value, 30);
        assert!(list.is_empty());
    }
}

#[test]
fn remove_head_and_tail() {
    let off = link_offset();

    let mut list: utils::list::intrusive::dlist::DList<Node> =
        utils::list::intrusive::dlist::DList::new_uninit();
    list.init();

    let mut n1 = new_node(1);
    let mut n2 = new_node(2);
    let mut n3 = new_node(3);
    let mut n4 = new_node(4);

    unsafe {
        list.push_back(link_ptr(&mut n1));
        list.push_back(link_ptr(&mut n2));
        list.push_back(link_ptr(&mut n3));
        list.push_back(link_ptr(&mut n4));

        list.remove(link_ptr(&mut n1));
        list.remove(link_ptr(&mut n4));

        let a = list.pop_front(off).unwrap();
        let b = list.pop_front(off).unwrap();

        assert_eq!((*a).value, 2);
        assert_eq!((*b).value, 3);
        assert!(list.is_empty());
    }

    assert!(n1.link.is_unlinked());
    assert!(n4.link.is_unlinked());
}

#[test]
fn reuse_node_after_removal() {
    let off = link_offset();

    let mut list: utils::list::intrusive::dlist::DList<Node> =
        utils::list::intrusive::dlist::DList::new_uninit();
    list.init();

    let mut n = new_node(42);

    unsafe {
        list.push_back(link_ptr(&mut n));

        let out = list.pop_front(off).unwrap();
        assert_eq!((*out).value, 42);
        assert!(n.link.is_unlinked());

        list.push_back(link_ptr(&mut n));

        let out2 = list.pop_front(off).unwrap();
        assert_eq!((*out2).value, 42);

        assert!(list.is_empty());
    }
}

#[test]
fn cursor_forward_remove_evens() {
    let off = link_offset();

    let mut list: utils::list::intrusive::dlist::DList<Node> =
        utils::list::intrusive::dlist::DList::new_uninit();
    list.init();

    let mut nodes = [
        new_node(1),
        new_node(2),
        new_node(3),
        new_node(4),
        new_node(5),
        new_node(6),
    ];

    unsafe {
        for n in &mut nodes {
            let p = link_ptr(n);
            list.push_back(p);
        }

        let mut cur = list.cursor_mut();
        while let Some((tptr, lptr)) = cur.next_with_offset(off) {
            let v = (*tptr).value;
            if v % 2 == 0 {
                cur.remove_link(lptr);
                assert!((*lptr).is_unlinked());
            }
        }

        let a = list.pop_front(off).unwrap();
        let b = list.pop_front(off).unwrap();
        let c = list.pop_front(off).unwrap();

        assert_eq!((*a).value, 1);
        assert_eq!((*b).value, 3);
        assert_eq!((*c).value, 5);
        assert!(list.is_empty());
    }
}

#[test]
fn cursor_backward_remove_odds() {
    let off = link_offset();

    let mut list: utils::list::intrusive::dlist::DList<Node> =
        utils::list::intrusive::dlist::DList::new_uninit();
    list.init();

    let mut nodes = [
        new_node(1),
        new_node(2),
        new_node(3),
        new_node(4),
        new_node(5),
    ];

    unsafe {
        for n in &mut nodes {
            let p = link_ptr(n);
            list.push_back(p);
        }

        let mut cur = list.cursor_back_mut();
        while let Some((tptr, lptr)) = cur.prev_with_offset(off) {
            let v = (*tptr).value;
            if v % 2 == 1 {
                cur.remove_link(lptr);
                assert!((*lptr).is_unlinked());
            }
        }

        let a = list.pop_front(off).unwrap();
        let b = list.pop_front(off).unwrap();

        assert_eq!((*a).value, 2);
        assert_eq!((*b).value, 4);
        assert!(list.is_empty());
    }
}

#[test]
fn remove_null_is_noop() {
    let off = link_offset();

    let mut list: utils::list::intrusive::dlist::DList<Node> =
        utils::list::intrusive::dlist::DList::new_uninit();
    list.init();

    let mut n1 = new_node(1);
    let mut n2 = new_node(2);

    unsafe {
        list.push_back(link_ptr(&mut n1));
        list.push_back(link_ptr(&mut n2));

        list.remove(ptr::null_mut());

        let a = list.pop_front(off).unwrap();
        let b = list.pop_front(off).unwrap();

        assert_eq!((*a).value, 1);
        assert_eq!((*b).value, 2);
        assert!(list.is_empty());
    }
}

// Debug-only tests: rely on debug_assert! panics.
#[cfg(debug_assertions)]
mod debug_only {
    use super::*;

    #[test]
    fn double_insert_panics() {
        let mut list: utils::list::intrusive::dlist::DList<Node> =
            utils::list::intrusive::dlist::DList::new_uninit();
        list.init();

        let mut n = new_node(7);

        unsafe {
            let p1 = link_ptr(&mut n);
            list.push_back(p1);

            let p2 = link_ptr(&mut n);
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                list.push_back(p2);
            }));
            assert!(result.is_err());
            while list.pop_front(link_offset()).is_some() {}
        }
    }

    #[test]
    #[should_panic(expected = "List not initialized")]
    fn uninitialized_list_panics_on_push() {
        let mut list: utils::list::intrusive::dlist::DList<Node> =
            utils::list::intrusive::dlist::DList::new_uninit();
        // list.init() intentionally omitted

        let mut n = new_node(1);

        unsafe {
            let p = link_ptr(&mut n);
            list.push_back(p);
        }
    }

    #[test]
    fn remove_unlinked_is_noop() {
        let mut list: utils::list::intrusive::dlist::DList<Node> =
            utils::list::intrusive::dlist::DList::new_uninit();
        list.init();

        let mut n = new_node(123);

        unsafe {
            let p = link_ptr(&mut n);
            list.remove(p);
        }

        assert!(list.is_empty());
    }
}
