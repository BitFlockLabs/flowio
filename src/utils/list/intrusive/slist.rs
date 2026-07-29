//! Minimal intrusive singly linked list used by free lists and slab chains.

use std::marker::PhantomData;
use std::ptr;

#[cfg(debug_assertions)]
macro_rules! debug_assert_slist_sanity {
    ($list:expr) => {{
        let mut slow = ($list).head;
        let mut fast = ($list).head;
        let mut count = 0;
        // Bounded Floyd cycle check; cap debug work on unexpectedly long lists.
        while !fast.is_null() && count < 1000 {
            unsafe {
                fast = (*fast).next;
                if fast.is_null() {
                    break;
                }
                fast = (*fast).next;
                slow = (*slow).next;

                debug_assert!(
                    slow != fast || slow.is_null(),
                    "singly linked list cycle detected"
                );
            }
            count += 1;
        }
    }};
}

#[cfg(not(debug_assertions))]
macro_rules! debug_assert_slist_sanity {
    ($list:expr) => {};
}

#[repr(C)]
/// Intrusive hook embedded in containers stored inside an [`SList`].
pub struct Link {
    /// Pointer to the next link in the list, or null when detached / at tail.
    pub next: *mut Link,
}

impl Link {
    /// Creates a detached link with no successor.
    pub const fn new_unlinked() -> Self {
        Self {
            next: ptr::null_mut(),
        }
    }

    #[inline(always)]
    /// Returns `true` when this link has no successor.
    ///
    /// A detached link and a link at the tail of a list both satisfy this
    /// condition, so callers must track list membership separately.
    pub fn is_unlinked(&self) -> bool {
        self.next.is_null()
    }
}

/// Minimal intrusive singly linked list used by free lists and slab chains.
///
/// Unlike [`super::dlist::DList`], this list has no self-referential sentinel
/// and is usable immediately after construction; [`SList::init`] is a no-op.
/// The list links but does not own or drop its containing nodes.
pub struct SList<T> {
    /// First link in the list, or null when empty.
    head: *mut Link,
    /// Carries the container type without storing values directly.
    _marker: PhantomData<T>,
}

impl<T> Default for SList<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> SList<T> {
    /// Creates an empty list in a state that is safe to move.
    pub const fn new() -> Self {
        Self {
            head: ptr::null_mut(),
            _marker: PhantomData,
        }
    }

    /// Alias for [`SList::new`], kept for symmetry with other intrusive
    /// structures that require a later `init`.
    pub const fn new_uninit() -> Self {
        Self::new()
    }

    /// No-op initialization hook; singly linked lists are immediately usable.
    pub fn init(&mut self) {}

    #[inline(always)]
    /// Returns `true` when the list contains no links.
    pub fn is_empty(&self) -> bool {
        self.head.is_null()
    }

    /// Pushes a detached link onto the front of the list.
    ///
    /// The debug-only null-successor check cannot distinguish a detached link
    /// from the tail of another list; membership remains a caller invariant.
    ///
    /// # Safety
    ///
    /// The caller must ensure that `node_link` is a valid, non-null pointer
    /// to a detached `Link`.
    #[inline(always)]
    pub unsafe fn push_front(&mut self, node_link: *mut Link) {
        debug_assert_slist_sanity!(self);
        debug_assert!(!node_link.is_null());
        debug_assert!(node_link != self.head, "slist double insert");
        debug_assert!(unsafe { (*node_link).is_unlinked() }, "slist double insert");

        unsafe {
            (*node_link).next = self.head;
            self.head = node_link;
        }
    }

    /// Pushes `node_link` to the front without inspecting its successor.
    ///
    /// Callers must track membership so the same link is never inserted into
    /// two lists or inserted twice into this list.
    ///
    /// # Safety
    ///
    /// The caller must ensure that `node_link` is a valid, non-null pointer
    /// to a `Link` that is not currently linked into any list.
    #[inline(always)]
    pub unsafe fn push_front_unchecked(&mut self, node_link: *mut Link) {
        debug_assert_slist_sanity!(self);
        debug_assert!(!node_link.is_null());
        debug_assert!(node_link != self.head, "attempted to push head onto itself");

        unsafe {
            (*node_link).next = self.head;
            self.head = node_link;
        }
    }

    /// Removes and returns the container at the front of the list.
    ///
    /// # Safety
    ///
    /// The caller must ensure that `T` begins with the intrusive `Link` field.
    #[inline(always)]
    pub unsafe fn pop_front(&mut self) -> Option<*mut T> {
        debug_assert_slist_sanity!(self);
        if self.is_empty() {
            return None;
        }
        unsafe {
            let node_ptr = self.head;
            self.head = (*node_ptr).next;
            (*node_ptr).next = ptr::null_mut();

            Some(node_ptr as *mut T)
        }
    }
}
