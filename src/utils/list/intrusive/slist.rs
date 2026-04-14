use std::marker::PhantomData;
use std::ptr;

#[cfg(debug_assertions)]
macro_rules! debug_assert_slist_sanity {
    ($list:expr) => {{
        let mut slow = ($list).head;
        let mut fast = ($list).head;
        let mut count = 0;
        // Floyd's cycle-finding algorithm (tortoise and hare)
        while !fast.is_null() && count < 1000
        {
            unsafe {
                fast = (*fast).next;
                if fast.is_null()
                {
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
pub struct Link
{
    /// Pointer to the next link in the list, or null when detached / at tail.
    pub next: *mut Link,
}

impl Link
{
    pub const fn new_unlinked() -> Self
    {
        Self {
            next: ptr::null_mut(),
        }
    }

    #[inline(always)]
    pub fn is_unlinked(&self) -> bool
    {
        self.next.is_null()
    }
}

pub struct SList<T>
{
    /// First link in the list, or null when empty.
    head: *mut Link,
    _marker: PhantomData<T>,
}

impl<T> Default for SList<T>
{
    fn default() -> Self
    {
        Self::new()
    }
}

impl<T> SList<T>
{
    pub const fn new() -> Self
    {
        Self {
            head: ptr::null_mut(),
            _marker: PhantomData,
        }
    }

    pub const fn new_uninit() -> Self
    {
        Self::new()
    }

    pub fn init(&mut self) {}

    #[inline(always)]
    pub fn is_empty(&self) -> bool
    {
        self.head.is_null()
    }

    #[inline(always)]
    /// # Safety
    ///
    /// The caller must ensure that `node_link` is a valid, non-null pointer
    /// to a `Link` that is currently unlinked (unless unchecked).
    pub unsafe fn push_front(&mut self, node_link: *mut Link)
    {
        debug_assert_slist_sanity!(self);
        debug_assert!(!node_link.is_null());
        debug_assert!(unsafe { (*node_link).is_unlinked() }, "slist double insert");

        unsafe {
            (*node_link).next = self.head;
            self.head = node_link;
        }
    }

    #[inline(always)]
    /// # Safety
    ///
    /// The caller must ensure that `node_link` is a valid, non-null pointer
    /// to a `Link` that is currently unlinked (unless unchecked).
    pub unsafe fn push_front_unchecked(&mut self, node_link: *mut Link)
    {
        debug_assert_slist_sanity!(self);
        debug_assert!(!node_link.is_null());
        debug_assert!(node_link != self.head, "attempted to push head onto itself");

        unsafe {
            (*node_link).next = self.head;
            self.head = node_link;
        }
    }

    #[inline(always)]
    /// # Safety
    ///
    /// The caller must ensure that the `offset` correctly represents the byte
    /// distance from the start of the container `T` to the `Link` field.
    pub unsafe fn pop_front(&mut self, offset: usize) -> Option<*mut T>
    {
        debug_assert_slist_sanity!(self);
        if self.is_empty()
        {
            return None;
        }
        unsafe {
            let node_ptr = self.head;
            self.head = (*node_ptr).next;
            (*node_ptr).next = ptr::null_mut();

            let container_ptr = (node_ptr as *mut u8).sub(offset) as *mut T;
            Some(container_ptr)
        }
    }
}
