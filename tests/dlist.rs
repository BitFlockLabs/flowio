use flowio::utils;
use std::mem::offset_of;
use std::ptr;

#[repr(C)]
pub struct Task
{
    pub id: u32,
    pub link: utils::list::intrusive::dlist::Link,
}

#[test]
fn test_verbose_pointer_audit()
{
    let offset = offset_of!(Task, link);

    // 1. Initialize List in a stable location
    let mut list = utils::list::intrusive::dlist::DList::<Task>::new_uninit();
    list.init();

    let head_addr = &list as *const _ as usize;
    println!("\n[AUDIT] List Initialized at: 0x{:x}", head_addr);

    // 2. Create a Slab of 10 tasks
    let mut slab: Vec<Task> = (0..10)
        .map(|i| Task {
            id: i,
            link: utils::list::intrusive::dlist::Link::new_unlinked(),
        })
        .collect();

    unsafe {
        println!("\n--- PHASE 1: SEQUENTIAL INSERTION (FIFO) ---");
        for task in slab.iter_mut()
        {
            let link_addr = &task.link as *const _ as usize;
            list.push_back(&mut task.link);
            println!(
                "INSERT: ID {:<2} | Link: 0x{:x} | (Prev: 0x{:x}, Next: 0x{:x})",
                task.id, link_addr, task.link.prev as usize, task.link.next as usize
            );
        }

        println!("\n--- PHASE 2: FORWARD WALK (Verification) ---");
        let mut cur = list.cursor_mut();
        while let Some((task, link_ptr)) = cur.next_with_offset(offset)
        {
            let link = &*link_ptr;
            println!(
                "VISIT: ID {:<2} | Self: 0x{:x} | Prev: 0x{:x} | Next: 0x{:x}",
                (*task).id,
                link_ptr as usize,
                link.prev as usize,
                link.next as usize
            );
        }

        println!("\n--- PHASE 3: SELECTIVE DELETION (Head, Mid, Tail) ---");
        // Remove 0 (Current Head), 5 (Middle), 9 (Current Tail)
        let targets = [0, 5, 9];
        for &idx in &targets
        {
            println!("REMOVING ID: {}", idx);
            list.remove(&mut slab[idx].link);
        }

        println!("\n--- PHASE 4: BACKWARD WALK (Post-Deletion) ---");
        let mut bcur = list.cursor_back_mut();
        while let Some((task, link_ptr)) = bcur.prev_with_offset(offset)
        {
            println!(
                "BACK-VISIT: ID {:<2} | Link: 0x{:x}",
                (*task).id,
                link_ptr as usize
            );
        }

        println!("\n--- PHASE 5: INTERLEAVED POP & RE-INSERT ---");
        // Pop 2 from front, push 1 new one to front
        for _ in 0..2
        {
            if let Some(t) = list.pop_front(offset)
            {
                println!("POP-FRONT: ID {}", (*t).id);
            }
        }

        // We reuse Task 0 (which was removed in Phase 3)
        println!("RE-INSERTING: ID 0 to Front");
        list.push_front(&mut slab[0].link);

        println!("\n--- FINAL LIST STATE ---");
        let mut cur = list.cursor_mut();
        while let Some((task, _)) = cur.next_with_offset(offset)
        {
            print!("{} <-> ", (*task).id);
        }
        println!("SENTINEL");
    }
}

#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "broken list")]
fn test_broken_list_detection()
{
    let mut list = utils::list::intrusive::dlist::DList::<Task>::new_uninit();
    list.init();

    let mut t1 = Task {
        id: 1,
        link: utils::list::intrusive::dlist::Link::new_unlinked(),
    };

    unsafe {
        list.push_back(&mut t1.link);

        // MANUALLY CORRUPT THE LIST (Simulating a stray pointer write)
        // We break the circular link: next.prev != head
        t1.link.next = std::ptr::null_mut();
        std::hint::black_box(&t1.link.next);

        // This should trigger your macro's internal consistency check
        list.push_back(
            &mut Task {
                id: 2,
                link: utils::list::intrusive::dlist::Link::new_unlinked(),
            }
            .link,
        );
    }
}

#[test]
fn test_verbose_splice_logic()
{
    let offset = std::mem::offset_of!(Task, link);

    // Setup two lists
    let mut running_queue = utils::list::intrusive::dlist::DList::<Task>::new_uninit();
    running_queue.init();
    let mut ready_queue = utils::list::intrusive::dlist::DList::<Task>::new_uninit();
    ready_queue.init();

    // Create 6 tasks: 3 for Running, 3 for Ready
    let mut tasks: Vec<Task> = (0..6)
        .map(|i| Task {
            id: i,
            link: utils::list::intrusive::dlist::Link::new_unlinked(),
        })
        .collect();

    unsafe {
        // Initial state
        println!("\n[PHASE 1] Initializing Queues");
        for task in tasks.iter_mut().take(3)
        {
            running_queue.push_back(&mut task.link);
        }
        for task in tasks.iter_mut().take(6).skip(3)
        {
            ready_queue.push_back(&mut task.link);
        }

        println!("Running Queue: ");
        print_list_verbose(&mut running_queue, offset);
        println!("Ready Queue: ");
        print_list_verbose(&mut ready_queue, offset);

        // Splice
        println!("\n[PHASE 2] Splicing Ready Queue into Running Queue (O(1))");
        running_queue.splice_back(&mut ready_queue);

        println!("Combined Running Queue:");
        print_list_verbose(&mut running_queue, offset);

        println!("Ready Queue (Post-Splice):");
        if ready_queue.is_empty()
        {
            println!("  (List is empty as expected)");
        }

        // Verify IDs are in order 0, 1, 2, 3, 4, 5
        let mut cur = running_queue.cursor_mut();
        let mut expected_id = 0;
        while let Some((task, _)) = cur.next_with_offset(offset)
        {
            assert_eq!((*task).id, expected_id);
            expected_id += 1;
        }
        assert_eq!(expected_id, 6);

        println!("\n[RESULT] Splice successful and order preserved.");
    }
}

/// Helper for verbose printing
unsafe fn print_list_verbose<T>(list: &mut utils::list::intrusive::dlist::DList<T>, offset: usize)
{
    let mut cur = list.cursor_mut();
    let mut found = false;
    while let Some((task, link)) = unsafe { cur.next_with_offset(offset) }
    {
        unsafe {
            let t = &*task as *const _ as *mut Task;
            println!("  ID: {} | LinkAddr: {:p}", (*t).id, link);
            found = true;
        }
    }
    if !found
    {
        println!("  <Empty>");
    }
}

#[repr(C)]
struct Node
{
    value: u32,
    link: utils::list::intrusive::dlist::Link,
}

fn new_node(value: u32) -> Node
{
    Node {
        value,
        link: utils::list::intrusive::dlist::Link::new_unlinked(),
    }
}

fn link_offset() -> usize
{
    offset_of!(Node, link)
}

fn link_ptr(n: &mut Node) -> *mut utils::list::intrusive::dlist::Link
{
    &mut n.link as *mut utils::list::intrusive::dlist::Link
}

#[test]
fn empty_uninit_then_init_verbose()
{
    let off = link_offset();
    eprintln!("\n== empty_uninit_then_init_verbose ==");
    eprintln!("offset(link) = {off}");

    let mut list: utils::list::intrusive::dlist::DList<Node> =
        utils::list::intrusive::dlist::DList::new_uninit();
    eprintln!(
        "created new_uninit list: is_empty={} (treats uninit as empty)",
        list.is_empty()
    );
    assert!(list.is_empty());

    list.init();
    eprintln!("after init: is_empty={}", list.is_empty());
    assert!(list.is_empty());
    assert!(list.front(off).is_none());

    eprintln!("PASS");
}

#[test]
fn front_peek_does_not_remove_verbose()
{
    let off = link_offset();
    eprintln!("\n== front_peek_does_not_remove_verbose ==");
    eprintln!("offset(link) = {off}");

    let mut list: utils::list::intrusive::dlist::DList<Node> =
        utils::list::intrusive::dlist::DList::new_uninit();
    list.init();

    let mut n1 = new_node(111);
    let p1 = link_ptr(&mut n1);
    eprintln!("push_back {}", n1.value);
    unsafe {
        list.push_back(p1);
    }

    // front() should not remove
    let f1 = list.front(off).unwrap();
    eprintln!("front -> {}", unsafe { (*f1).value });
    assert_eq!(unsafe { (*f1).value }, 111);
    assert!(!list.is_empty());

    let f2 = list.front(off).unwrap();
    eprintln!("front again -> {}", unsafe { (*f2).value });
    assert_eq!(unsafe { (*f2).value }, 111);
    assert!(!list.is_empty());

    let popped = unsafe { list.pop_front(off).unwrap() };
    eprintln!("pop_front -> {}", unsafe { (*popped).value });
    assert_eq!(unsafe { (*popped).value }, 111);
    assert!(list.is_empty());
    assert!(n1.link.is_unlinked());

    eprintln!("PASS");
}

#[test]
fn push_back_order_then_pop_front_verbose()
{
    let off = link_offset();
    eprintln!("\n== push_back_order_then_pop_front_verbose ==");
    eprintln!("offset(link) = {off}");

    let mut list: utils::list::intrusive::dlist::DList<Node> =
        utils::list::intrusive::dlist::DList::new_uninit();
    list.init();

    let mut n1 = new_node(1);
    let mut n2 = new_node(2);
    let mut n3 = new_node(3);

    {
        let p = link_ptr(&mut n1);
        eprintln!("n1 value={} link={:p}", n1.value, p);
    }
    {
        let p = link_ptr(&mut n2);
        eprintln!("n2 value={} link={:p}", n2.value, p);
    }
    {
        let p = link_ptr(&mut n3);
        eprintln!("n3 value={} link={:p}", n3.value, p);
    }

    unsafe {
        let p1 = link_ptr(&mut n1);
        eprintln!("push_back {}", n1.value);
        list.push_back(p1);

        let p2 = link_ptr(&mut n2);
        eprintln!("push_back {}", n2.value);
        list.push_back(p2);

        let p3 = link_ptr(&mut n3);
        eprintln!("push_back {}", n3.value);
        list.push_back(p3);

        let a = list.pop_front(off).unwrap();
        eprintln!("pop_front -> {}", (*a).value);
        assert_eq!((*a).value, 1);

        let b = list.pop_front(off).unwrap();
        eprintln!("pop_front -> {}", (*b).value);
        assert_eq!((*b).value, 2);

        let c = list.pop_front(off).unwrap();
        eprintln!("pop_front -> {}", (*c).value);
        assert_eq!((*c).value, 3);

        eprintln!("pop_front -> None");
        assert!(list.pop_front(off).is_none());
        assert!(list.is_empty());
    }

    assert!(n1.link.is_unlinked());
    assert!(n2.link.is_unlinked());
    assert!(n3.link.is_unlinked());
    eprintln!("PASS");
}

#[test]
fn push_front_order_then_pop_front_verbose()
{
    let off = link_offset();
    eprintln!("\n== push_front_order_then_pop_front_verbose ==");
    eprintln!("offset(link) = {off}");

    let mut list: utils::list::intrusive::dlist::DList<Node> =
        utils::list::intrusive::dlist::DList::new_uninit();
    list.init();

    let mut n1 = new_node(1);
    let mut n2 = new_node(2);
    let mut n3 = new_node(3);

    unsafe {
        eprintln!("push_front {}", n1.value);
        list.push_front(link_ptr(&mut n1)); // [1]
        eprintln!("front -> {:?}", list.front(off).map(|p| (*p).value));

        eprintln!("push_front {}", n2.value);
        list.push_front(link_ptr(&mut n2)); // [2,1]
        eprintln!("front -> {:?}", list.front(off).map(|p| (*p).value));

        eprintln!("push_front {}", n3.value);
        list.push_front(link_ptr(&mut n3)); // [3,2,1]
        eprintln!("front -> {:?}", list.front(off).map(|p| (*p).value));

        let a = list.pop_front(off).unwrap();
        let b = list.pop_front(off).unwrap();
        let c = list.pop_front(off).unwrap();
        eprintln!(
            "popped order: {}, {}, {}",
            (*a).value,
            (*b).value,
            (*c).value
        );

        assert_eq!((*a).value, 3);
        assert_eq!((*b).value, 2);
        assert_eq!((*c).value, 1);
        assert!(list.is_empty());
    }

    eprintln!("PASS");
}

#[test]
fn mixed_pushes_expected_order_verbose()
{
    let off = link_offset();
    eprintln!("\n== mixed_pushes_expected_order_verbose ==");
    eprintln!("offset(link) = {off}");

    let mut list: utils::list::intrusive::dlist::DList<Node> =
        utils::list::intrusive::dlist::DList::new_uninit();
    list.init();

    let mut n1 = new_node(1);
    let mut n2 = new_node(2);
    let mut n3 = new_node(3);

    unsafe {
        eprintln!("push_back {}", n1.value); // [1]
        list.push_back(link_ptr(&mut n1));
        eprintln!("push_back {}", n2.value); // [1,2]
        list.push_back(link_ptr(&mut n2));
        eprintln!("push_front {}", n3.value); // [3,1,2]
        list.push_front(link_ptr(&mut n3));

        let a = list.pop_front(off).unwrap();
        let b = list.pop_front(off).unwrap();
        let c = list.pop_front(off).unwrap();
        eprintln!(
            "popped order: {}, {}, {}",
            (*a).value,
            (*b).value,
            (*c).value
        );

        assert_eq!((*a).value, 3);
        assert_eq!((*b).value, 1);
        assert_eq!((*c).value, 2);
        assert!(list.pop_front(off).is_none());
        assert!(list.is_empty());
    }

    eprintln!("PASS");
}

#[test]
fn remove_arbitrary_middle_verbose()
{
    let off = link_offset();
    eprintln!("\n== remove_arbitrary_middle_verbose ==");
    eprintln!("offset(link) = {off}");

    let mut list: utils::list::intrusive::dlist::DList<Node> =
        utils::list::intrusive::dlist::DList::new_uninit();
    list.init();

    let mut n1 = new_node(10);
    let mut n2 = new_node(20);
    let mut n3 = new_node(30);

    unsafe {
        eprintln!("push_back 10,20,30");
        list.push_back(link_ptr(&mut n1));
        list.push_back(link_ptr(&mut n2));
        list.push_back(link_ptr(&mut n3));

        let p2 = link_ptr(&mut n2);
        eprintln!("remove {}", n2.value);
        list.remove(p2);

        eprintln!("n2.link.is_unlinked={}", n2.link.is_unlinked());
        assert!(n2.link.is_unlinked());

        let a = list.pop_front(off).unwrap();
        let b = list.pop_front(off).unwrap();
        eprintln!("remaining popped: {}, {}", (*a).value, (*b).value);

        assert_eq!((*a).value, 10);
        assert_eq!((*b).value, 30);
        assert!(list.is_empty());
    }

    eprintln!("PASS");
}

#[test]
fn remove_head_and_tail_verbose()
{
    let off = link_offset();
    eprintln!("\n== remove_head_and_tail_verbose ==");
    eprintln!("offset(link) = {off}");

    let mut list: utils::list::intrusive::dlist::DList<Node> =
        utils::list::intrusive::dlist::DList::new_uninit();
    list.init();

    let mut n1 = new_node(1);
    let mut n2 = new_node(2);
    let mut n3 = new_node(3);
    let mut n4 = new_node(4);

    unsafe {
        eprintln!("push_back 1,2,3,4");
        list.push_back(link_ptr(&mut n1));
        list.push_back(link_ptr(&mut n2));
        list.push_back(link_ptr(&mut n3));
        list.push_back(link_ptr(&mut n4));

        eprintln!("remove head (1)");
        list.remove(link_ptr(&mut n1));
        eprintln!("remove tail (4)");
        list.remove(link_ptr(&mut n4));

        let a = list.pop_front(off).unwrap();
        let b = list.pop_front(off).unwrap();
        eprintln!("remaining popped: {}, {}", (*a).value, (*b).value);

        assert_eq!((*a).value, 2);
        assert_eq!((*b).value, 3);
        assert!(list.is_empty());
    }

    assert!(n1.link.is_unlinked());
    assert!(n4.link.is_unlinked());
    eprintln!("PASS");
}

#[test]
fn reuse_node_after_removal_verbose()
{
    let off = link_offset();
    eprintln!("\n== reuse_node_after_removal_verbose ==");
    eprintln!("offset(link) = {off}");

    let mut list: utils::list::intrusive::dlist::DList<Node> =
        utils::list::intrusive::dlist::DList::new_uninit();
    list.init();

    let mut n = new_node(42);

    unsafe {
        eprintln!("push_back {}", n.value);
        list.push_back(link_ptr(&mut n));

        eprintln!("pop_front");
        let out = list.pop_front(off).unwrap();
        eprintln!("popped {}", (*out).value);
        assert_eq!((*out).value, 42);
        assert!(n.link.is_unlinked());

        eprintln!("reinsert same node");
        list.push_back(link_ptr(&mut n));

        eprintln!("pop_front again");
        let out2 = list.pop_front(off).unwrap();
        eprintln!("popped {}", (*out2).value);
        assert_eq!((*out2).value, 42);

        assert!(list.is_empty());
    }

    eprintln!("PASS");
}

#[test]
fn cursor_forward_remove_evens_verbose()
{
    let off = link_offset();
    eprintln!("\n== cursor_forward_remove_evens_verbose ==");
    eprintln!("offset(link) = {off}");

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
        eprintln!("push_back 1..6");
        for n in &mut nodes
        {
            let p = link_ptr(n);
            let v = n.value;
            eprintln!("  push {} link={:p}", v, p);
            list.push_back(p);
        }

        eprintln!("iterate forward and remove evens");
        let mut cur = list.cursor_mut();
        while let Some((tptr, lptr)) = cur.next_with_offset(off)
        {
            let v = (*tptr).value;
            eprintln!("  visit {} link={:p}", v, lptr);
            if v % 2 == 0
            {
                eprintln!("    REMOVE {}", v);
                cur.remove_link(lptr);
                eprintln!("    removed link is_unlinked={}", (*lptr).is_unlinked());
                assert!((*lptr).is_unlinked());
            }
        }

        eprintln!("pop remaining (expect 1,3,5)");
        let a = list.pop_front(off).unwrap();
        let b = list.pop_front(off).unwrap();
        let c = list.pop_front(off).unwrap();
        eprintln!("  popped {}, {}, {}", (*a).value, (*b).value, (*c).value);

        assert_eq!((*a).value, 1);
        assert_eq!((*b).value, 3);
        assert_eq!((*c).value, 5);
        assert!(list.is_empty());
    }

    eprintln!("PASS");
}

#[test]
fn cursor_backward_remove_odds_verbose()
{
    let off = link_offset();
    eprintln!("\n== cursor_backward_remove_odds_verbose ==");
    eprintln!("offset(link) = {off}");

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
        eprintln!("push_back 1..5");
        for n in &mut nodes
        {
            let p = link_ptr(n);
            let v = n.value;
            eprintln!("  push {} link={:p}", v, p);
            list.push_back(p);
        }

        eprintln!("iterate backward and remove odds");
        let mut cur = list.cursor_back_mut();
        while let Some((tptr, lptr)) = cur.prev_with_offset(off)
        {
            let v = (*tptr).value;
            eprintln!("  visit {} link={:p}", v, lptr);
            if v % 2 == 1
            {
                eprintln!("    REMOVE {}", v);
                cur.remove_link(lptr);
                eprintln!("    removed link is_unlinked={}", (*lptr).is_unlinked());
                assert!((*lptr).is_unlinked());
            }
        }

        eprintln!("pop remaining (expect 2,4)");
        let a = list.pop_front(off).unwrap();
        let b = list.pop_front(off).unwrap();
        eprintln!("  popped {}, {}", (*a).value, (*b).value);

        assert_eq!((*a).value, 2);
        assert_eq!((*b).value, 4);
        assert!(list.is_empty());
    }

    eprintln!("PASS");
}

#[test]
fn remove_null_is_noop_verbose()
{
    let off = link_offset();
    eprintln!("\n== remove_null_is_noop_verbose ==");
    eprintln!("offset(link) = {off}");

    let mut list: utils::list::intrusive::dlist::DList<Node> =
        utils::list::intrusive::dlist::DList::new_uninit();
    list.init();

    let mut n1 = new_node(1);
    let mut n2 = new_node(2);

    unsafe {
        eprintln!("push_back 1,2");
        list.push_back(link_ptr(&mut n1));
        list.push_back(link_ptr(&mut n2));

        eprintln!("remove(NULL)");
        list.remove(ptr::null_mut());

        let a = list.pop_front(off).unwrap();
        let b = list.pop_front(off).unwrap();
        eprintln!("popped {}, {}", (*a).value, (*b).value);

        assert_eq!((*a).value, 1);
        assert_eq!((*b).value, 2);
        assert!(list.is_empty());
    }

    eprintln!("PASS");
}

// Debug-only tests: rely on debug_assert! panics.
#[cfg(debug_assertions)]
mod debug_only
{
    use super::*;

    #[test]
    #[should_panic(expected = "double insert")]
    fn double_insert_panics_verbose()
    {
        eprintln!("\n== double_insert_panics_verbose ==");

        let mut list: utils::list::intrusive::dlist::DList<Node> =
            utils::list::intrusive::dlist::DList::new_uninit();
        list.init();

        let mut n = new_node(7);

        unsafe {
            let p1 = link_ptr(&mut n);
            eprintln!("push_back first time link={:p}", p1);
            list.push_back(p1);

            let p2 = link_ptr(&mut n);
            eprintln!("push_back second time (panic expected) link={:p}", p2);
            list.push_back(p2);
        }
    }

    #[test]
    #[should_panic(expected = "List not initialized")]
    fn uninitialized_list_panics_on_push_verbose()
    {
        eprintln!("\n== uninitialized_list_panics_on_push_verbose ==");

        let mut list: utils::list::intrusive::dlist::DList<Node> =
            utils::list::intrusive::dlist::DList::new_uninit();
        // list.init() intentionally omitted

        let mut n = new_node(1);

        unsafe {
            let p = link_ptr(&mut n);
            eprintln!(
                "push_back on uninitialized list (panic expected) link={:p}",
                p
            );
            list.push_back(p);
        }
    }

    #[test]
    #[should_panic(expected = "remove on unlinked node")]
    fn remove_unlinked_panics_verbose()
    {
        eprintln!("\n== remove_unlinked_panics_verbose ==");

        let mut list: utils::list::intrusive::dlist::DList<Node> =
            utils::list::intrusive::dlist::DList::new_uninit();
        list.init();

        let mut n = new_node(123);

        unsafe {
            let p = link_ptr(&mut n);
            eprintln!("remove on unlinked node (panic expected) link={:p}", p);
            list.remove(p);
        }
    }
}
