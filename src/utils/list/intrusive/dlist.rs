//! Circular intrusive doubly linked list used by ready queues and timer
//! buckets.

use std::marker::PhantomData;
use std::ptr;

// Disabled under Miri: the sentinel cross-checks dereference head-derived raw
// pointers in a way that conflicts with Miri's stricter provenance model.
// Regular debug builds still run these sentinel consistency checks.
#[cfg(all(debug_assertions, not(miri)))]
macro_rules! debug_assert_list_inited {
        ($list:expr) => {{
            let h: *mut Link = &($list).head as *const Link as *mut Link;
            // Must be initialized: empty lists use null `next` and non-null
            // `prev`; non-empty lists use circular payload links.
            debug_assert!(
                unsafe { !(*h).prev.is_null() },
                "List not initialized: call init() after final placement"
            );
            if unsafe { (*h).next.is_null() } {
                debug_assert_eq!(unsafe { (*h).prev }, h, "broken empty list: prev != head - Maybe list moved after initialization");
            } else {
                // Must be internally consistent (basic sentinel sanity)
                debug_assert_eq!(unsafe { (*(*h).next).prev }, h, "broken list: next.prev != head - Maybe list not initialized or moved after initialization");
                debug_assert_eq!(unsafe { (*(*h).prev).next }, h, "broken list: prev.next != head - Maybe list not initialized or moved after initialization");
            }
        }};
    }

#[cfg(any(not(debug_assertions), miri))]
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
    /// Returns `true` when both list pointers mark this link as detached.
    pub fn is_unlinked(&self) -> bool {
        self.next.is_null() && self.prev.is_null()
    }
}

/// A circular intrusive doubly linked list.
///
/// The list links but does not own or drop its containing nodes. Initialize it
/// after final placement; a non-empty initialized list must not move.
///
/// # Container-pointer contract
///
/// Offset-based operations recover `*mut T` by subtracting a link-field offset
/// from the stored pointer. Every inserted link pointer that may be recovered
/// this way must therefore be derived from a pointer with provenance for the
/// complete containing allocation, advanced by the exact
/// `offset_of!(T, chosen_link)`. A pointer produced by taking a reference to
/// only the embedded link field does not satisfy this recovery contract.
///
/// Each list must consistently use the same embedded [`Link`] and matching
/// offset, although another list may use a different link in the same `T`.
/// The containing allocation and chosen link must remain live and at the same
/// address for the entire time that link remains linked; recovery that does
/// not unlink the node does not shorten this requirement. A returned `*mut T`
/// must not be dereferenced unless it identifies a valid initialized `T`.
///
/// # Example
///
/// ```
/// # #[cfg(feature = "test-support")]
/// # {
/// use flowio::test_support::utils::list::intrusive::dlist::{DList, Link};
/// use std::mem::offset_of;
///
/// #[repr(C)]
/// struct Entry {
///     value: u32,
///     link: Link,
/// }
///
/// let mut entry = Box::new(Entry {
///     value: 7,
///     link: Link::new_unlinked(),
/// });
/// let entry_ptr: *mut Entry = &mut *entry;
/// let link_offset = offset_of!(Entry, link);
/// // SAFETY: the checked field offset stays within the complete boxed entry.
/// let link_ptr = unsafe {
///     entry_ptr.cast::<u8>().add(link_offset).cast::<Link>()
/// };
/// let mut list = DList::<Entry>::new_uninit();
/// list.init();
///
/// // SAFETY: the pointer retains whole-entry provenance, the exact same
/// // offset is used for recovery, and both the entry and list stay in place.
/// unsafe {
///     list.push_back(link_ptr);
///     let recovered = list.pop_front(link_offset).unwrap();
///     assert_eq!(recovered, entry_ptr);
///     assert_eq!((*recovered).value, 7);
/// }
/// # }
/// ```
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

    /// Initializes the sentinel so the list can accept payload links.
    pub fn init(&mut self) {
        let head_ptr = &mut self.head as *mut Link;
        self.set_empty(head_ptr);
    }

    /// Returns `true` when the list is uninitialized or has no payload nodes.
    ///
    /// In debug builds, callers are still expected to initialize the list
    /// before use.
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        let head_ptr = &self.head as *const Link;
        self.head.next.is_null() || std::ptr::eq(self.head.next as *const Link, head_ptr)
    }

    /// Internal helper that links `new_link` between two adjacent nodes.
    #[inline(always)]
    /// # Safety
    ///
    /// The caller must ensure `new_link`, `prev`, `next`, and `head` are valid
    /// pointers from the same initialized list.
    unsafe fn __list_add(new_link: *mut Link, prev: *mut Link, next: *mut Link, head: *mut Link) {
        let prev = Self::normalize_head_ptr(prev, head);
        let next = Self::normalize_head_ptr(next, head);
        unsafe {
            (*next).prev = new_link;
            (*new_link).next = next;
            (*new_link).prev = prev;
            (*prev).next = new_link;
        }
    }

    /// Canonicalizes `link` to the list's authoritative `head` pointer.
    ///
    /// Stored `next`/`prev` pointers that numerically identify the sentinel
    /// can carry provenance from an earlier borrow. Mapping such pointers back
    /// to the current `head` pointer keeps relink writes based on the active
    /// mutable borrow. This does not make a non-empty initialized list movable.
    #[inline(always)]
    fn normalize_head_ptr(link: *mut Link, head: *mut Link) -> *mut Link {
        if link == head { head } else { link }
    }

    /// Marks the list initialized and empty without a self-reference in
    /// `next`, so moving an empty list after init does not leave a stale
    /// pointer that teardown must dereference.
    #[inline(always)]
    fn set_empty(&mut self, head: *mut Link) {
        self.head.next = ptr::null_mut();
        self.head.prev = head;
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

    /// Shared tail-insertion implementation for the checked and unchecked APIs.
    #[inline(always)]
    /// # Safety
    ///
    /// The list must be initialized and remain at its stable address.
    /// `node_link` must be valid, non-null, currently unlinked, and satisfy the
    /// whole-allocation provenance and stable-storage requirements documented
    /// on [`DList`]. `CHECK_DETACHED` controls only the debug diagnostic; it
    /// does not weaken the caller's unlinked-node obligation.
    unsafe fn push_back_inner<const CHECK_DETACHED: bool>(&mut self, node_link: *mut Link) {
        debug_assert_list_inited!(self);

        let head_ptr = &mut self.head as *mut Link;
        let prev = if self.head.next.is_null() {
            head_ptr
        } else {
            unsafe { Self::normalize_head_ptr((*head_ptr).prev, head_ptr) }
        };

        debug_assert!(!node_link.is_null());
        if CHECK_DETACHED {
            debug_assert!(unsafe { (*node_link).is_unlinked() }, "double insert");
        }

        debug_assert!(node_link != head_ptr, "attempted to insert sentinel");

        unsafe {
            Self::__list_add(node_link, prev, head_ptr, head_ptr);
        }
    }

    /// Adds an element to the back of the list.
    #[inline(always)]
    /// # Safety
    ///
    /// The caller must ensure that `node_link` is a valid, non-null pointer
    /// to a currently unlinked `Link` and satisfies the whole-allocation
    /// provenance and stable-storage requirements documented on [`DList`].
    pub unsafe fn push_back(&mut self, node_link: *mut Link) {
        unsafe {
            self.push_back_inner::<true>(node_link);
        }
    }

    /// Adds an element to the back of the list without checking whether it is
    /// already linked. Callers must guarantee queue membership independently.
    #[inline(always)]
    /// # Safety
    ///
    /// The caller must ensure that `node_link` is valid, non-null, and not
    /// currently linked into any list, and satisfies the whole-allocation
    /// provenance and stable-storage requirements documented on [`DList`].
    pub unsafe fn push_back_unchecked(&mut self, node_link: *mut Link) {
        unsafe {
            self.push_back_inner::<false>(node_link);
        }
    }

    /// Shared front-insertion implementation for the checked and unchecked APIs.
    #[inline(always)]
    /// # Safety
    ///
    /// The list must be initialized and remain at its stable address.
    /// `node_link` must be valid, non-null, currently unlinked, and satisfy the
    /// whole-allocation provenance and stable-storage requirements documented
    /// on [`DList`]. `CHECK_DETACHED` controls only the debug diagnostic; it
    /// does not weaken the caller's unlinked-node obligation.
    unsafe fn push_front_inner<const CHECK_DETACHED: bool>(&mut self, node_link: *mut Link) {
        debug_assert_list_inited!(self);

        let head_ptr = &mut self.head as *mut Link;
        let next = if self.head.next.is_null() {
            head_ptr
        } else {
            unsafe { Self::normalize_head_ptr((*head_ptr).next, head_ptr) }
        };

        debug_assert!(!node_link.is_null());
        if CHECK_DETACHED {
            debug_assert!(unsafe { (*node_link).is_unlinked() }, "double insert");
        }

        debug_assert!(node_link != head_ptr, "attempted to insert sentinel");

        unsafe {
            Self::__list_add(node_link, head_ptr, next, head_ptr);
        }
    }

    /// Adds an element to the front of the list.
    #[inline(always)]
    /// # Safety
    ///
    /// The caller must ensure that `node_link` is a valid, non-null pointer
    /// to a currently unlinked `Link` and satisfies the whole-allocation
    /// provenance and stable-storage requirements documented on [`DList`].
    #[cfg(any(test, feature = "test-support"))]
    pub unsafe fn push_front(&mut self, node_link: *mut Link) {
        unsafe {
            self.push_front_inner::<true>(node_link);
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
    /// to a currently unlinked `Link` and satisfies the whole-allocation
    /// provenance and stable-storage requirements documented on [`DList`].
    pub unsafe fn push_front_unchecked(&mut self, node_link: *mut Link) {
        unsafe {
            self.push_front_inner::<false>(node_link);
        }
    }

    /// Unlinks every payload node without requiring the container offset.
    ///
    /// This is for owner teardown paths that are already discarding the
    /// backing storage for every linked node. Normal list users should remove
    /// or pop nodes explicitly so ownership remains visible. A fully
    /// uninitialized list sentinel is tolerated for defensive teardown, but a
    /// partially initialized sentinel is rejected in debug builds. Initialized
    /// non-empty lists must not move after `init`; moved-empty sentinels remain
    /// tolerated for teardown. Unlike [`DList::drain_all_for_drop`], this
    /// offset-free helper cannot re-anchor a moved non-empty sentinel without
    /// dereferencing its stale boundary, so it deliberately retains the
    /// non-move contract.
    pub(crate) fn unlink_all_for_drop(&mut self) {
        let next_null = self.head.next.is_null();
        let prev_null = self.head.prev.is_null();
        debug_assert!(
            next_null || !prev_null,
            "DList teardown saw a partially initialized sentinel"
        );
        if next_null && prev_null {
            return;
        }

        let head_ptr = &mut self.head as *mut Link;
        if next_null {
            self.set_empty(head_ptr);
            return;
        }
        if prev_null {
            return;
        }

        let mut current = Self::normalize_head_ptr(self.head.next, head_ptr);
        while current != head_ptr {
            unsafe {
                let next = Self::normalize_head_ptr((*current).next, head_ptr);
                (*current).next = ptr::null_mut();
                (*current).prev = ptr::null_mut();
                if next.is_null() {
                    break;
                }
                current = next;
            }
        }

        self.set_empty(head_ptr);
    }

    /// Drains every payload node for owner teardown, passing each container
    /// pointer to `f` after unlinking its node.
    ///
    /// This is for drop paths that must return node storage to an owner before
    /// the list itself is discarded. It tolerates a sentinel that was moved
    /// after initialization by re-anchoring the first and last payload links
    /// to the current sentinel before draining. This is intentionally stronger
    /// than [`DList::unlink_all_for_drop`], whose offset-free teardown contract
    /// requires a non-empty initialized list to remain in place.
    ///
    /// Each node is fully detached and responsibility for it transfers to `f`
    /// before `f` runs. If `f` unwinds, this list neither reclaims nor relinks
    /// that node: `f` must already have consumed it or the caller must retain a
    /// separate recovery handle. Later nodes remain linked and can be drained
    /// by a caller that catches the panic.
    ///
    /// # Safety
    ///
    /// The caller must ensure `offset` is the byte distance from `T` to its
    /// embedded [`Link`]. Each linked node must satisfy the whole-allocation
    /// provenance and stable-storage contract documented on [`DList`] until
    /// it is detached and transferred to `f`; `f` may then consume that node.
    pub(crate) unsafe fn drain_all_for_drop<F>(&mut self, offset: usize, mut f: F)
    where
        F: FnMut(*mut T),
    {
        let next_null = self.head.next.is_null();
        let prev_null = self.head.prev.is_null();
        debug_assert!(
            next_null || !prev_null,
            "DList teardown saw a partially initialized sentinel"
        );
        if next_null && prev_null {
            return;
        }

        let head_ptr = &mut self.head as *mut Link;
        if next_null {
            self.set_empty(head_ptr);
            return;
        }
        if prev_null {
            return;
        }

        let first = self.head.next;
        if first == head_ptr {
            self.set_empty(head_ptr);
            return;
        }

        // A teardown caller may have moved the sentinel after initialization.
        // The payload allocations did not move, so repair only their boundary
        // links without dereferencing the stale sentinel address.
        let last = self.head.prev;
        unsafe {
            (*first).prev = head_ptr;
            (*last).next = head_ptr;
        }

        while let Some(container_ptr) = unsafe { self.pop_front(offset) } {
            f(container_ptr);
        }
    }

    /// Removes a specific node from the list.
    #[inline(always)]
    /// # Safety
    ///
    /// The caller must ensure that `node_link` is a valid pointer to a `Link`
    /// that is currently part of this specific list. Its containing allocation
    /// and chosen link must satisfy the stable-storage requirements documented
    /// on [`DList`] through removal.
    pub unsafe fn remove(&mut self, node_link: *mut Link) {
        if node_link.is_null() {
            return;
        }

        debug_assert_list_inited!(self);

        let head_ptr = &mut self.head as *mut Link;
        if node_link == head_ptr {
            return;
        }

        unsafe {
            let next = Self::normalize_head_ptr((*node_link).next, head_ptr);
            let prev = Self::normalize_head_ptr((*node_link).prev, head_ptr);

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

            if next == head_ptr && prev == head_ptr {
                self.set_empty(head_ptr);
            } else {
                (*next).prev = prev;
                (*prev).next = next;
            }

            // Clear pointers to mark as unlinked.
            (*node_link).next = ptr::null_mut();
            (*node_link).prev = ptr::null_mut();
        }
    }

    /// Returns the first element without unlinking it.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the `offset` correctly represents the byte
    /// distance from the start of the container `T` to the [`Link`] field used
    /// by this list. Every stored link must satisfy the whole-allocation
    /// provenance and stable-storage contract documented on [`DList`]. Because
    /// this method does not unlink the node, that contract continues after the
    /// returned pointer is produced.
    #[inline(always)]
    pub unsafe fn front(&self, offset: usize) -> Option<*mut T> {
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

    /// Returns the last payload link without unlinking it.
    ///
    /// The returned pointer remains valid only while its containing node stays
    /// live and linked in this list.
    #[inline(always)]
    pub(crate) fn back_link(&self) -> Option<*mut Link> {
        debug_assert_list_inited!(self);

        if self.is_empty() {
            None
        } else {
            Some(self.head.prev)
        }
    }

    /// Returns the payload link immediately before `node_link`, or `None` when
    /// `node_link` is the first payload node.
    ///
    /// # Safety
    ///
    /// `node_link` must be non-null and currently linked in this specific
    /// initialized list. Its containing allocation must stay live and unmoved
    /// for the duration of the call.
    #[inline(always)]
    pub(crate) unsafe fn previous_link(&self, node_link: *mut Link) -> Option<*mut Link> {
        debug_assert_list_inited!(self);
        debug_assert!(!node_link.is_null());

        #[cfg(debug_assertions)]
        {
            debug_assert!(
                self.contains_link(node_link),
                "previous_link on node that does not belong to this list"
            );
        }

        let head_ptr = &self.head as *const Link as *mut Link;
        let previous = unsafe { Self::normalize_head_ptr((*node_link).prev, head_ptr) };
        if previous == head_ptr {
            None
        } else {
            Some(previous)
        }
    }

    /// Removes and returns the first element.
    #[inline(always)]
    /// # Safety
    ///
    /// The caller must ensure that the `offset` correctly represents the byte
    /// distance from the start of the container `T` to the [`Link`] field.
    /// Every stored link must satisfy the whole-allocation provenance and
    /// stable-storage contract documented on [`DList`] through removal.
    pub unsafe fn pop_front(&mut self, offset: usize) -> Option<*mut T> {
        unsafe { self.pop_front_with_empty(offset).0 }
    }

    /// Removes the first element and reports whether the list is then empty.
    ///
    /// Returning the post-pop state lets internal queue consumers update
    /// parallel occupancy metadata without probing the sentinel a second time.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the `offset` correctly represents the byte
    /// distance from the start of the container `T` to the [`Link`] field.
    /// Every stored link must satisfy the whole-allocation provenance and
    /// stable-storage contract documented on [`DList`] through removal.
    #[inline(always)]
    pub(crate) unsafe fn pop_front_with_empty(&mut self, offset: usize) -> (Option<*mut T>, bool) {
        debug_assert_list_inited!(self);

        if self.is_empty() {
            return (None, true);
        }

        unsafe {
            let node_ptr = self.head.next;
            let container_ptr = (node_ptr as *mut u8).sub(offset) as *mut T;

            let head_ptr = &mut self.head as *mut Link;
            let next = Self::normalize_head_ptr((*node_ptr).next, head_ptr);
            let prev = Self::normalize_head_ptr((*node_ptr).prev, head_ptr);
            let empty_after_pop = next == head_ptr && prev == head_ptr;

            if empty_after_pop {
                self.set_empty(head_ptr);
            } else {
                (*next).prev = prev;
                (*prev).next = next;
            }

            (*node_ptr).next = ptr::null_mut();
            (*node_ptr).prev = ptr::null_mut();

            (Some(container_ptr), empty_after_pop)
        }
    }

    /// Returns a mutable forward cursor over the list.
    #[cfg(any(test, feature = "test-support"))]
    pub fn cursor_mut(&mut self) -> CursorMut<'_, T> {
        debug_assert_list_inited!(self);

        let head_ptr = &mut self.head as *mut Link;
        CursorMut {
            current: if self.head.next.is_null() {
                head_ptr
            } else {
                unsafe { Self::normalize_head_ptr((*head_ptr).next, head_ptr) }
            },
            head: head_ptr,
            list: self,
        }
    }

    /// Returns a backward-walking iterator.
    #[cfg(any(test, feature = "test-support"))]
    pub fn cursor_back_mut(&mut self) -> CursorBackMut<'_, T> {
        debug_assert_list_inited!(self);

        let head_ptr = &mut self.head as *mut Link;
        CursorBackMut {
            current: if self.head.next.is_null() {
                head_ptr
            } else {
                unsafe { Self::normalize_head_ptr((*head_ptr).prev, head_ptr) }
            },
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
#[cfg(any(test, feature = "test-support"))]
pub struct CursorMut<'a, T> {
    /// Link that will be yielded on the next cursor step.
    current: *mut Link,
    /// Sentinel head used to detect the end of the circular list.
    head: *mut Link,
    /// Borrowed list that this cursor can also mutate via removals.
    list: &'a mut DList<T>,
}

#[cfg(any(test, feature = "test-support"))]
impl<'a, T> CursorMut<'a, T> {
    #[inline(always)]
    /// # Safety
    ///
    /// The caller must ensure that the `offset` correctly represents the byte
    /// distance from the start of the container `T` to the [`Link`] field.
    /// Every stored link must satisfy the whole-allocation provenance and
    /// stable-storage contract documented on [`DList`]. This step does not
    /// unlink the node, so that contract continues after recovery.
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
    /// The caller must ensure that `link` is non-null and points to a live
    /// payload `Link`, not the sentinel, that is currently part of this
    /// cursor's specific initialized list. The containing allocation must
    /// remain valid and unmoved through removal, with no aliased mutation of
    /// the list or link. Removing the link that would be yielded next is
    /// supported: that node is skipped and forward iteration continues.
    pub unsafe fn remove_link(&mut self, link: *mut Link) {
        unsafe {
            if link == self.current {
                self.current = DList::<T>::normalize_head_ptr((*link).next, self.head);
            }
            self.list.remove(link);
        }
    }
}

/// Mutable backward cursor over a [`DList`].
#[cfg(any(test, feature = "test-support"))]
pub struct CursorBackMut<'a, T> {
    /// Link that will be yielded on the next backward cursor step.
    current: *mut Link,
    /// Sentinel head used to detect the end of the circular list.
    head: *mut Link,
    /// Borrowed list that this cursor can also mutate via removals.
    _list: &'a mut DList<T>,
}

#[cfg(any(test, feature = "test-support"))]
impl<'a, T> CursorBackMut<'a, T> {
    #[inline(always)]
    /// # Safety
    ///
    /// The caller must ensure that the `offset` correctly represents the byte
    /// distance from the start of the container `T` to the [`Link`] field.
    /// Every stored link must satisfy the whole-allocation provenance and
    /// stable-storage contract documented on [`DList`] through recovery.
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
    /// The caller must ensure that `link` is non-null and points to a live
    /// payload `Link`, not the sentinel, that is currently part of this
    /// cursor's specific initialized list. The containing allocation must
    /// remain valid and unmoved through removal, with no aliased mutation of
    /// the list or link. Removing the link that would be yielded next is
    /// supported: that node is skipped and backward iteration continues.
    pub unsafe fn remove_link(&mut self, link: *mut Link) {
        unsafe {
            if link == self.current {
                self.current = DList::<T>::normalize_head_ptr((*link).prev, self.head);
            }
            self._list.remove(link);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::mem::offset_of;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::rc::Rc;

    #[repr(C)]
    struct Node {
        id: u32,
        link: Link,
    }

    #[repr(C)]
    struct DropNode {
        id: u32,
        link: Link,
        drops: Rc<RefCell<Vec<u32>>>,
    }

    impl Drop for DropNode {
        fn drop(&mut self) {
            self.drops.borrow_mut().push(self.id);
        }
    }

    #[derive(Debug)]
    struct DrainDropPanic;

    fn node_link_ptr(node: &mut Node) -> *mut Link {
        let base = node as *mut Node as *mut u8;
        unsafe { base.add(offset_of!(Node, link)) as *mut Link }
    }

    unsafe fn drop_node_link_ptr(node: *mut DropNode) -> *mut Link {
        unsafe { std::ptr::addr_of_mut!((*node).link) }
    }

    #[derive(Clone, Copy)]
    enum CursorDirection {
        Forward,
        Backward,
    }

    fn assert_cursor_removal(
        direction: CursorDirection,
        linked_count: usize,
        steps_before_remove: usize,
        remove_index: usize,
        expected_after_remove: &[u32],
    ) {
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
        let links = [
            node_link_ptr(&mut nodes[0]),
            node_link_ptr(&mut nodes[1]),
            node_link_ptr(&mut nodes[2]),
        ];

        unsafe {
            for &link in links.iter().take(linked_count) {
                list.push_back(link);
            }

            match direction {
                CursorDirection::Forward => {
                    let mut cursor = list.cursor_mut();
                    for step in 0..steps_before_remove {
                        let (node, _) = cursor
                            .next_with_offset(offset_of!(Node, link))
                            .expect("forward cursor should reach removal position");
                        assert_eq!((*node).id, step as u32 + 1);
                    }
                    cursor.remove_link(links[remove_index]);
                    assert!((*links[remove_index]).is_unlinked());
                    for expected in expected_after_remove {
                        let (node, _) = cursor
                            .next_with_offset(offset_of!(Node, link))
                            .expect("forward cursor ended before expected node");
                        assert_eq!((*node).id, *expected);
                    }
                    assert!(
                        cursor.next_with_offset(offset_of!(Node, link)).is_none(),
                        "forward cursor repeated or skipped past the expected end"
                    );
                }
                CursorDirection::Backward => {
                    let mut cursor = list.cursor_back_mut();
                    for step in 0..steps_before_remove {
                        let (node, _) = cursor
                            .prev_with_offset(offset_of!(Node, link))
                            .expect("backward cursor should reach removal position");
                        assert_eq!((*node).id, linked_count as u32 - step as u32);
                    }
                    cursor.remove_link(links[remove_index]);
                    assert!((*links[remove_index]).is_unlinked());
                    for expected in expected_after_remove {
                        let (node, _) = cursor
                            .prev_with_offset(offset_of!(Node, link))
                            .expect("backward cursor ended before expected node");
                        assert_eq!((*node).id, *expected);
                    }
                    assert!(
                        cursor.prev_with_offset(offset_of!(Node, link)).is_none(),
                        "backward cursor repeated or skipped past the expected end"
                    );
                }
            }
        }

        list.unlink_all_for_drop();
        assert!(nodes.iter().all(|node| node.link.is_unlinked()));
    }

    #[test]
    fn cursor_remove_next_advances_before_unlinking() {
        let forward_cases: &[(usize, usize, &[u32])] =
            &[(0, 0, &[2, 3]), (1, 1, &[3]), (2, 2, &[]), (1, 2, &[2])];
        for &(steps, remove_index, expected) in forward_cases {
            assert_cursor_removal(CursorDirection::Forward, 3, steps, remove_index, expected);
        }
        assert_cursor_removal(CursorDirection::Forward, 1, 0, 0, &[]);

        let backward_cases: &[(usize, usize, &[u32])] =
            &[(0, 2, &[2, 1]), (1, 1, &[1]), (2, 0, &[]), (1, 0, &[2])];
        for &(steps, remove_index, expected) in backward_cases {
            assert_cursor_removal(CursorDirection::Backward, 3, steps, remove_index, expected);
        }
        assert_cursor_removal(CursorDirection::Backward, 1, 0, 0, &[]);
    }

    #[test]
    fn dlist_size_matches_sentinel_link() {
        assert_eq!(
            std::mem::size_of::<DList<Node>>(),
            std::mem::size_of::<Link>()
        );
        assert_eq!(
            std::mem::align_of::<DList<Node>>(),
            std::mem::align_of::<Link>()
        );
    }

    #[test]
    fn checked_and_unchecked_push_back_preserve_fifo_transitions() {
        let mut checked = DList::<Node>::new_uninit();
        checked.init();
        let mut unchecked = DList::<Node>::new_uninit();
        unchecked.init();
        let mut checked_nodes: [Node; 3] = std::array::from_fn(|index| Node {
            id: index as u32 + 1,
            link: Link::new_unlinked(),
        });
        let mut unchecked_nodes: [Node; 3] = std::array::from_fn(|index| Node {
            id: index as u32 + 1,
            link: Link::new_unlinked(),
        });
        let checked_links: [*mut Link; 3] =
            std::array::from_fn(|index| node_link_ptr(&mut checked_nodes[index]));
        let unchecked_links: [*mut Link; 3] =
            std::array::from_fn(|index| node_link_ptr(&mut unchecked_nodes[index]));

        unsafe {
            checked.push_back(checked_links[0]);
            unchecked.push_back_unchecked(unchecked_links[0]);
            assert_eq!(checked.back_link(), Some(checked_links[0]));
            assert_eq!(unchecked.back_link(), Some(unchecked_links[0]));

            for index in 1..checked_links.len() {
                checked.push_back(checked_links[index]);
                unchecked.push_back_unchecked(unchecked_links[index]);
            }
            assert_eq!(checked.back_link(), Some(checked_links[2]));
            assert_eq!(unchecked.back_link(), Some(unchecked_links[2]));

            for expected in 1..=3 {
                let checked_node = checked
                    .pop_front(offset_of!(Node, link))
                    .expect("checked list ended before the expected node");
                let unchecked_node = unchecked
                    .pop_front(offset_of!(Node, link))
                    .expect("unchecked list ended before the expected node");
                assert_eq!((*checked_node).id, expected);
                assert_eq!((*unchecked_node).id, expected);
            }
        }

        assert!(checked.is_empty());
        assert!(unchecked.is_empty());
        assert!(checked_nodes.iter().all(|node| node.link.is_unlinked()));
        assert!(unchecked_nodes.iter().all(|node| node.link.is_unlinked()));
    }

    #[test]
    fn checked_and_unchecked_push_front_preserve_lifo_transitions() {
        let mut checked = DList::<Node>::new_uninit();
        checked.init();
        let mut unchecked = DList::<Node>::new_uninit();
        unchecked.init();
        let mut checked_nodes: [Node; 3] = std::array::from_fn(|index| Node {
            id: index as u32 + 1,
            link: Link::new_unlinked(),
        });
        let mut unchecked_nodes: [Node; 3] = std::array::from_fn(|index| Node {
            id: index as u32 + 1,
            link: Link::new_unlinked(),
        });
        let checked_links: [*mut Link; 3] =
            std::array::from_fn(|index| node_link_ptr(&mut checked_nodes[index]));
        let unchecked_links: [*mut Link; 3] =
            std::array::from_fn(|index| node_link_ptr(&mut unchecked_nodes[index]));

        unsafe {
            for index in 0..checked_links.len() {
                checked.push_front(checked_links[index]);
                unchecked.push_front_unchecked(unchecked_links[index]);

                let checked_front = checked
                    .front(offset_of!(Node, link))
                    .expect("checked list lost its front node");
                let unchecked_front = unchecked
                    .front(offset_of!(Node, link))
                    .expect("unchecked list lost its front node");
                assert_eq!((*checked_front).id, index as u32 + 1);
                assert_eq!((*unchecked_front).id, index as u32 + 1);
            }

            for expected in (1..=3).rev() {
                let checked_node = checked
                    .pop_front(offset_of!(Node, link))
                    .expect("checked list ended before the expected node");
                let unchecked_node = unchecked
                    .pop_front(offset_of!(Node, link))
                    .expect("unchecked list ended before the expected node");
                assert_eq!((*checked_node).id, expected);
                assert_eq!((*unchecked_node).id, expected);
            }
        }

        assert!(checked.is_empty());
        assert!(unchecked.is_empty());
        assert!(checked_nodes.iter().all(|node| node.link.is_unlinked()));
        assert!(unchecked_nodes.iter().all(|node| node.link.is_unlinked()));
    }

    #[cfg(debug_assertions)]
    #[test]
    fn checked_push_front_rejects_double_insert() {
        let mut list = DList::<Node>::new_uninit();
        list.init();
        let mut node = Node {
            id: 1,
            link: Link::new_unlinked(),
        };
        let link = node_link_ptr(&mut node);

        unsafe {
            list.push_front(link);
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                list.push_front(link);
            }));
            assert!(result.is_err());
            assert_eq!(
                list.pop_front(offset_of!(Node, link)),
                Some(std::ptr::addr_of_mut!(node))
            );
        }

        assert!(list.is_empty());
        assert!(node.link.is_unlinked());
    }

    #[test]
    fn pop_front_with_empty_reports_empty_list() {
        let mut list = DList::<Node>::new_uninit();
        list.init();

        let (item, empty_after_pop) = unsafe { list.pop_front_with_empty(offset_of!(Node, link)) };

        assert!(item.is_none());
        assert!(empty_after_pop);
        assert!(list.is_empty());
    }

    #[test]
    fn pop_front_with_empty_reports_singleton_transition() {
        let mut list = DList::<Node>::new_uninit();
        list.init();
        let mut node = Node {
            id: 1,
            link: Link::new_unlinked(),
        };
        let node_ptr = std::ptr::addr_of_mut!(node);

        unsafe {
            list.push_back(node_link_ptr(&mut node));
        }
        let (item, empty_after_pop) = unsafe { list.pop_front_with_empty(offset_of!(Node, link)) };

        assert_eq!(item, Some(node_ptr));
        assert!(empty_after_pop);
        assert!(node.link.is_unlinked());
        assert!(list.is_empty());
    }

    #[test]
    fn pop_front_with_empty_reports_multi_node_transitions() {
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
                list.push_back(node_link_ptr(node));
            }

            for (expected_id, expected_empty) in [(1, false), (2, false), (3, true)] {
                let (item, empty_after_pop) = list.pop_front_with_empty(offset_of!(Node, link));
                let item = item.expect("list ended before the expected node");
                assert_eq!((*item).id, expected_id);
                assert_eq!(empty_after_pop, expected_empty);
            }
        }

        assert!(nodes.iter().all(|node| node.link.is_unlinked()));
        assert!(list.is_empty());
    }

    #[test]
    fn back_and_previous_links_stop_at_the_payload_boundary() {
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
        let links = [
            node_link_ptr(&mut nodes[0]),
            node_link_ptr(&mut nodes[1]),
            node_link_ptr(&mut nodes[2]),
        ];

        assert_eq!(list.back_link(), None);
        unsafe {
            list.push_back(links[0]);
            assert_eq!(list.back_link(), Some(links[0]));
            assert_eq!(list.previous_link(links[0]), None);

            list.push_back(links[1]);
            list.push_back(links[2]);
            assert_eq!(list.back_link(), Some(links[2]));
            assert_eq!(list.previous_link(links[2]), Some(links[1]));
            assert_eq!(list.previous_link(links[1]), Some(links[0]));
            assert_eq!(list.previous_link(links[0]), None);
        }

        list.unlink_all_for_drop();
    }

    #[test]
    fn unlink_all_for_drop_handles_empty_list() {
        let mut list = DList::<Node>::new_uninit();
        list.init();

        list.unlink_all_for_drop();

        assert!(list.is_empty());
    }

    #[test]
    fn unlink_all_for_drop_handles_moved_empty_list() {
        let mut list = DList::<Node>::new_uninit();
        list.init();

        let mut moved = list;
        moved.unlink_all_for_drop();

        assert!(moved.is_empty());
    }

    #[test]
    fn drain_all_for_drop_handles_moved_empty_list() {
        let mut list = DList::<Node>::new_uninit();
        list.init();

        let mut moved = list;
        let mut drained = false;
        unsafe {
            moved.drain_all_for_drop(offset_of!(Node, link), |_| {
                drained = true;
            });
        }

        assert!(!drained, "empty moved list should not drain payloads");
        assert!(moved.is_empty());
    }

    #[test]
    fn drain_all_for_drop_preserves_remaining_list_after_callback_frees_and_panics() {
        let drops = Rc::new(RefCell::new(Vec::new()));
        let nodes = [1, 2, 3].map(|id| {
            Box::into_raw(Box::new(DropNode {
                id,
                link: Link::new_unlinked(),
                drops: Rc::clone(&drops),
            }))
        });
        let mut list = DList::<DropNode>::new_uninit();
        list.init();
        unsafe {
            for node in nodes {
                list.push_back(drop_node_link_ptr(node));
            }
        }

        let unwind = catch_unwind(AssertUnwindSafe(|| unsafe {
            list.drain_all_for_drop(offset_of!(DropNode, link), |node| {
                let id = (*node).id;
                drop(Box::from_raw(node));
                if id == 1 {
                    std::panic::panic_any(DrainDropPanic);
                }
            });
        }))
        .expect_err("first consuming drain callback did not panic");
        assert!(
            unwind.downcast_ref::<DrainDropPanic>().is_some(),
            "drain cleanup replaced the original panic"
        );
        assert_eq!(*drops.borrow(), [1]);

        let front = unsafe {
            list.front(offset_of!(DropNode, link))
                .expect("remaining list lost its second node")
        };
        assert_eq!(unsafe { (*front).id }, 2);

        let mut remaining = Vec::new();
        unsafe {
            list.drain_all_for_drop(offset_of!(DropNode, link), |node| {
                remaining.push((*node).id);
                drop(Box::from_raw(node));
            });
        }
        assert_eq!(remaining, [2, 3]);
        assert_eq!(*drops.borrow(), [1, 2, 3]);
        assert!(list.is_empty());

        let replacement = Box::into_raw(Box::new(DropNode {
            id: 4,
            link: Link::new_unlinked(),
            drops: Rc::clone(&drops),
        }));
        unsafe {
            list.push_back(drop_node_link_ptr(replacement));
            let popped = list
                .pop_front(offset_of!(DropNode, link))
                .expect("reused list did not return its replacement node");
            assert_eq!(popped, replacement);
            drop(Box::from_raw(popped));
        }
        assert!(list.is_empty());
        assert_eq!(*drops.borrow(), [1, 2, 3, 4]);
    }

    #[test]
    fn drain_all_for_drop_reanchors_moved_nonempty_sentinel() {
        let drops = Rc::new(RefCell::new(Vec::new()));
        let nodes = [1, 2, 3].map(|id| {
            Box::into_raw(Box::new(DropNode {
                id,
                link: Link::new_unlinked(),
                drops: Rc::clone(&drops),
            }))
        });
        let mut list = DList::<DropNode>::new_uninit();
        list.init();
        unsafe {
            for node in nodes {
                list.push_back(drop_node_link_ptr(node));
            }
        }

        let mut moved = list;
        let mut drained = Vec::new();
        unsafe {
            moved.drain_all_for_drop(offset_of!(DropNode, link), |node| {
                drained.push((*node).id);
                drop(Box::from_raw(node));
            });
        }

        assert_eq!(drained, [1, 2, 3]);
        assert_eq!(*drops.borrow(), [1, 2, 3]);
        assert!(moved.is_empty());
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
            list.push_back(node_link_ptr(&mut node));
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
                list.push_back(node_link_ptr(node));
            }
        }

        list.unlink_all_for_drop();
        assert!(list.is_empty());
        assert!(nodes.iter().all(|node| node.link.is_unlinked()));

        unsafe {
            list.push_back(node_link_ptr(&mut nodes[1]));
            let popped = list
                .pop_front(offset_of!(Node, link))
                .expect("reused list should pop inserted node");
            assert_eq!((*popped).id, 2);
        }
        assert!(list.is_empty());
        assert!(nodes[1].link.is_unlinked());
    }
}
