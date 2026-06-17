//! Circular intrusive doubly linked list used by ready queues and timer
//! buckets.

use std::marker::PhantomData;
use std::ptr;

#[cfg(debug_assertions)]
macro_rules! debug_assert_list_inited {
        ($list:expr) => {{
            let h: *mut Link = &($list).head as *const Link as *mut Link;
            // Must be initialized: next/prev not null
            debug_assert!(
                unsafe { !(*h).next.is_null() && !(*h).prev.is_null() },
                "List not initialized: call init() after final placement"
            );
            // Must be internally consistent (basic sentinel sanity)
            debug_assert_eq!(unsafe { (*(*h).next).prev }, h, "broken list: next.prev != head - Maybe list not initialized or moved after initialization");
            debug_assert_eq!(unsafe { (*(*h).prev).next }, h, "broken list: prev.next != head - Maybe list not initialized or moved after initialization");
        }};
    }

#[cfg(not(debug_assertions))]
macro_rules! debug_assert_list_inited {
    ($list:expr) => {};
}

/// The intrusive hook that must be embedded within your struct.
#[repr(C)]
pub struct Link {
    /// Next link in the circular list.
    pub next: *mut Link,
    /// Previous link in the circular list.
    pub prev: *mut Link,
}

impl Link {
    /// Creates a Link in a detached state (pointers are null).
    pub const fn new_unlinked() -> Self {
        Self {
            next: ptr::null_mut(),
            prev: ptr::null_mut(),
        }
    }

    #[inline(always)]
    pub fn is_unlinked(&self) -> bool {
        self.next.is_null() && self.prev.is_null()
    }
}

/// A circular intrusive doubly linked list.
pub struct DList<T> {
    /// Sentinel head node anchoring the circular list.
    head: Link,
    /// Carries the container type without storing values directly.
    _marker: PhantomData<T>,
}

impl<T> DList<T> {
    /// Creates a new list instance.
    /// Call [`DList::init`] after moving it to its final memory location.
    pub const fn new_uninit() -> Self {
        Self {
            head: Link::new_unlinked(),
            _marker: PhantomData,
        }
    }

    /// Initializes the sentinel so the list points to itself.
    pub fn init(&mut self) {
        let head_ptr = &mut self.head as *mut Link;
        unsafe {
            (*head_ptr).next = head_ptr;
            (*head_ptr).prev = head_ptr;
        }
    }

    /// Returns `true` when the list is uninitialized or has no payload nodes.
    ///
    /// In debug builds, callers are still expected to initialize the list
    /// before use.
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        let head_ptr = &self.head as *const Link;
        self.head.next.is_null() || std::ptr::eq(self.head.next, head_ptr)
    }

    /// Internal helper that links `new_link` between two adjacent nodes.
    #[inline(always)]
    /// # Safety
    ///
    /// The caller must ensure `new_link`, `prev`, and `next` are valid pointers.
    unsafe fn __list_add(&mut self, new_link: *mut Link, prev: *mut Link, next: *mut Link) {
        debug_assert_list_inited!(self);
        unsafe {
            (*next).prev = new_link;
            (*new_link).next = next;
            (*new_link).prev = prev;
            (*prev).next = new_link;
        }
    }

    #[cfg(debug_assertions)]
    fn contains_link(&self, node_link: *mut Link) -> bool {
        // Debug-only ownership check. Release builds keep remove O(1); debug
        // builds spend O(n) here to catch cross-list removal bugs early.
        if node_link.is_null() || self.head.next.is_null() || self.head.prev.is_null() {
            return false;
        }

        let head_ptr = &self.head as *const Link as *mut Link;
        let mut current = self.head.next;
        while current != head_ptr {
            if current == node_link {
                return true;
            }
            unsafe {
                current = (*current).next;
            }
            if current.is_null() {
                return false;
            }
        }
        false
    }

    /// Adds an element to the back of the list.
    #[inline(always)]
    /// # Safety
    ///
    /// The caller must ensure that `node_link` is a valid, non-null pointer
    /// to a currently unlinked `Link`.
    pub unsafe fn push_back(&mut self, node_link: *mut Link) {
        debug_assert_list_inited!(self);

        let head_ptr = &mut self.head as *mut Link;

        debug_assert!(!node_link.is_null());
        debug_assert!(unsafe { (*node_link).is_unlinked() }, "double insert");

        debug_assert!(node_link != head_ptr, "attempted to insert sentinel");

        unsafe {
            self.__list_add(node_link, (*head_ptr).prev, head_ptr);
        }
    }

    /// Adds an element to the back of the list without checking whether it is
    /// already linked. Callers must guarantee queue membership independently.
    #[inline(always)]
    /// # Safety
    ///
    /// The caller must ensure that `node_link` is valid, non-null, and not
    /// currently linked into any list.
    pub unsafe fn push_back_unchecked(&mut self, node_link: *mut Link) {
        debug_assert_list_inited!(self);

        let head_ptr = &mut self.head as *mut Link;

        debug_assert!(!node_link.is_null());
        debug_assert!(node_link != head_ptr, "attempted to insert sentinel");

        unsafe {
            self.__list_add(node_link, (*head_ptr).prev, head_ptr);
        }
    }

    /// Adds an element to the front of the list.
    #[inline(always)]
    /// # Safety
    ///
    /// The caller must ensure that `node_link` is a valid, non-null pointer
    /// to a currently unlinked `Link`.
    pub unsafe fn push_front(&mut self, node_link: *mut Link) {
        debug_assert_list_inited!(self);

        let head_ptr = &mut self.head as *mut Link;

        debug_assert!(!node_link.is_null());
        debug_assert!(unsafe { (*node_link).is_unlinked() }, "double insert");

        debug_assert!(node_link != head_ptr, "attempted to insert sentinel");

        unsafe {
            self.__list_add(node_link, head_ptr, (*head_ptr).next);
        }
    }

    /// Adds an element to the front of the list without checking its detached
    /// state first.
    ///
    /// This is used in internal fast paths where queue membership is already
    /// tracked by surrounding state.
    #[inline(always)]
    /// # Safety
    ///
    /// The caller must ensure that `node_link` is a valid, non-null pointer
    /// to a currently unlinked `Link`.
    pub unsafe fn push_front_unchecked(&mut self, node_link: *mut Link) {
        debug_assert_list_inited!(self);

        let head_ptr = &mut self.head as *mut Link;

        debug_assert!(!node_link.is_null());
        debug_assert!(node_link != head_ptr, "attempted to insert sentinel");

        unsafe {
            self.__list_add(node_link, head_ptr, (*head_ptr).next);
        }
    }

    /// Appends all nodes from `other` to the back of `self` in O(1).
    ///
    /// This is reserved low-level infrastructure for queue-splice paths that
    /// need to move an entire list without per-node relinking.
    ///
    /// Both lists must be initialized, and `self` and `other` must be distinct
    /// lists. After this returns, `other` is empty and reusable.
    #[inline(always)]
    pub fn append_back(&mut self, other: &mut Self) {
        debug_assert_list_inited!(self);
        debug_assert_list_inited!(other);
        debug_assert!(
            !std::ptr::eq(self, other),
            "DList::append_back requires distinct lists"
        );

        if other.is_empty() {
            return;
        }

        let self_head = &mut self.head as *mut Link;
        let other_head = &mut other.head as *mut Link;

        unsafe {
            let first = (*other_head).next;
            let last = (*other_head).prev;
            let self_last = (*self_head).prev;

            (*self_last).next = first;
            (*first).prev = self_last;
            (*last).next = self_head;
            (*self_head).prev = last;

            (*other_head).next = other_head;
            (*other_head).prev = other_head;
        }
    }

    /// Unlinks every payload node without requiring the container offset.
    ///
    /// This is for owner teardown paths that are already discarding the
    /// backing storage for every linked node. Normal list users should remove
    /// or pop nodes explicitly so ownership remains visible. A fully
    /// uninitialized list sentinel is tolerated for defensive teardown, but a
    /// partially initialized sentinel is rejected in debug builds.
    pub(crate) fn unlink_all_for_drop(&mut self) {
        let next_null = self.head.next.is_null();
        let prev_null = self.head.prev.is_null();
        debug_assert_eq!(
            next_null, prev_null,
            "DList teardown saw a partially initialized sentinel"
        );
        if next_null || prev_null {
            return;
        }

        let head_ptr = &mut self.head as *mut Link;
        let mut current = self.head.next;
        while current != head_ptr {
            unsafe {
                let next = (*current).next;
                (*current).next = ptr::null_mut();
                (*current).prev = ptr::null_mut();
                if next.is_null() {
                    break;
                }
                current = next;
            }
        }

        self.head.next = head_ptr;
        self.head.prev = head_ptr;
    }

    /// Removes a specific node from the list.
    #[inline(always)]
    /// # Safety
    ///
    /// The caller must ensure that `node_link` is a valid pointer to a `Link`
    /// that is currently part of this specific list.
    pub unsafe fn remove(&mut self, node_link: *mut Link) {
        if node_link.is_null() {
            return;
        }

        debug_assert_list_inited!(self);

        let head_ptr = &self.head as *const Link as *mut Link;
        if node_link == head_ptr {
            return;
        }

        unsafe {
            let next = (*node_link).next;
            let prev = (*node_link).prev;

            if next.is_null() || prev.is_null() {
                return;
            }

            #[cfg(debug_assertions)]
            {
                debug_assert!(
                    self.contains_link(node_link),
                    "remove on node that does not belong to this list"
                );
            }

            (*next).prev = prev;
            (*prev).next = next;

            // Clear pointers to mark as unlinked.
            (*node_link).next = ptr::null_mut();
            (*node_link).prev = ptr::null_mut();
        }
    }

    /// Returns the first element without unlinking it.
    ///
    /// `offset` must be the byte distance from the start of `T` to the
    /// embedded [`Link`] field used by this list.
    #[inline(always)]
    pub fn front(&self, offset: usize) -> Option<*mut T> {
        debug_assert_list_inited!(self);

        if self.is_empty() {
            return None;
        }
        unsafe {
            let node_ptr = self.head.next;
            let container_ptr = (node_ptr as *mut u8).sub(offset) as *mut T;
            debug_assert_eq!(
                ((container_ptr as *mut u8).add(offset) as *mut Link),
                node_ptr
            );
            Some(container_ptr)
        }
    }

    /// Removes and returns the first element.
    #[inline(always)]
    /// # Safety
    ///
    /// The caller must ensure that the `offset` correctly represents the byte
    /// distance from the start of the container `T` to the `Link` field.
    pub unsafe fn pop_front(&mut self, offset: usize) -> Option<*mut T> {
        debug_assert_list_inited!(self);

        if self.is_empty() {
            return None;
        }

        unsafe {
            let node_ptr = self.head.next;
            let container_ptr = (node_ptr as *mut u8).sub(offset) as *mut T;

            let next = (*node_ptr).next;
            let prev = (*node_ptr).prev;

            (*next).prev = prev;
            (*prev).next = next;

            (*node_ptr).next = ptr::null_mut();
            (*node_ptr).prev = ptr::null_mut();

            Some(container_ptr)
        }
    }

    /// Returns a forward-walking iterator
    pub fn cursor_mut(&mut self) -> CursorMut<'_, T> {
        debug_assert_list_inited!(self);

        let head_ptr = &mut self.head as *mut Link;
        CursorMut {
            current: unsafe { (*head_ptr).next },
            head: head_ptr,
            list: self,
        }
    }

    /// Returns a backward-walking iterator.
    pub fn cursor_back_mut(&mut self) -> CursorBackMut<'_, T> {
        debug_assert_list_inited!(self);

        let head_ptr = &mut self.head as *mut Link;
        CursorBackMut {
            current: unsafe { (*head_ptr).prev },
            head: head_ptr,
            _list: self,
        }
    }
}

impl<T> Drop for DList<T> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            return;
        }

        debug_assert!(
            self.head.next.is_null() || self.is_empty(),
            "DList dropped while still containing linked nodes"
        );
    }
}

/// Mutable forward cursor over a [`DList`].
pub struct CursorMut<'a, T> {
    /// Link that will be yielded on the next cursor step.
    current: *mut Link,
    /// Sentinel head used to detect the end of the circular list.
    head: *mut Link,
    /// Borrowed list that this cursor can also mutate via removals.
    list: &'a mut DList<T>,
}

impl<'a, T> CursorMut<'a, T> {
    #[inline(always)]
    /// # Safety
    ///
    /// The caller must ensure that the `offset` correctly represents the byte
    /// distance from the start of the container `T` to the `Link` field.
    pub unsafe fn next_with_offset(&mut self, offset: usize) -> Option<(*mut T, *mut Link)> {
        if self.current == self.head {
            return None;
        }

        unsafe {
            let node_ptr = self.current;
            self.current = (*node_ptr).next;

            let container_ptr = (node_ptr as *mut u8).sub(offset) as *mut T;
            debug_assert_eq!(
                ((container_ptr as *mut u8).add(offset) as *mut Link),
                node_ptr
            );
            Some((container_ptr, node_ptr))
        }
    }

    #[inline(always)]
    /// # Safety
    ///
    /// The caller must ensure that `node_link` is a valid pointer to a `Link`
    /// that is currently part of this specific list.
    pub unsafe fn remove_link(&mut self, link: *mut Link) {
        unsafe {
            self.list.remove(link);
        }
    }
}

/// Mutable backward cursor over a [`DList`].
pub struct CursorBackMut<'a, T> {
    /// Link that will be yielded on the next backward cursor step.
    current: *mut Link,
    /// Sentinel head used to detect the end of the circular list.
    head: *mut Link,
    /// Borrowed list that this cursor can also mutate via removals.
    _list: &'a mut DList<T>,
}

impl<'a, T> CursorBackMut<'a, T> {
    #[inline(always)]
    /// # Safety
    ///
    /// The caller must ensure that the `offset` correctly represents the byte
    /// distance from the start of the container `T` to the `Link` field.
    pub unsafe fn prev_with_offset(&mut self, offset: usize) -> Option<(*mut T, *mut Link)> {
        if self.current == self.head {
            return None;
        }

        unsafe {
            let node_ptr = self.current;
            self.current = (*node_ptr).prev;

            let container_ptr = (node_ptr as *mut u8).sub(offset) as *mut T;
            debug_assert_eq!(
                ((container_ptr as *mut u8).add(offset) as *mut Link),
                node_ptr
            );

            Some((container_ptr, node_ptr))
        }
    }

    #[inline(always)]
    /// # Safety
    ///
    /// The caller must ensure that `node_link` is a valid pointer to a `Link`
    /// that is currently part of this specific list.
    pub unsafe fn remove_link(&mut self, link: *mut Link) {
        unsafe {
            self._list.remove(link);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::offset_of;

    #[repr(C)]
    struct Node {
        id: u32,
        link: Link,
    }

    #[test]
    fn unlink_all_for_drop_handles_empty_list() {
        let mut list = DList::<Node>::new_uninit();
        list.init();

        list.unlink_all_for_drop();

        assert!(list.is_empty());
    }

    #[test]
    fn unlink_all_for_drop_detaches_single_node() {
        let mut list = DList::<Node>::new_uninit();
        list.init();
        let mut node = Node {
            id: 1,
            link: Link::new_unlinked(),
        };

        unsafe {
            list.push_back(&mut node.link);
        }

        list.unlink_all_for_drop();

        assert!(list.is_empty());
        assert!(node.link.is_unlinked());
    }

    #[test]
    fn unlink_all_for_drop_detaches_nodes_and_reuses_list() {
        let mut list = DList::<Node>::new_uninit();
        list.init();
        let mut nodes = [
            Node {
                id: 1,
                link: Link::new_unlinked(),
            },
            Node {
                id: 2,
                link: Link::new_unlinked(),
            },
            Node {
                id: 3,
                link: Link::new_unlinked(),
            },
        ];

        unsafe {
            for node in &mut nodes {
                list.push_back(&mut node.link);
            }
        }

        list.unlink_all_for_drop();
        assert!(list.is_empty());
        assert!(nodes.iter().all(|node| node.link.is_unlinked()));

        unsafe {
            list.push_back(&mut nodes[1].link);
            let popped = list
                .pop_front(offset_of!(Node, link))
                .expect("reused list should pop inserted node");
            assert_eq!((*popped).id, 2);
        }
        assert!(list.is_empty());
        assert!(nodes[1].link.is_unlinked());
    }
}
