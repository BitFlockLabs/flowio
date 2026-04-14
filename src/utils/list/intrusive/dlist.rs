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
pub struct Link
{
    /// Next link in the circular list.
    pub next: *mut Link,
    /// Previous link in the circular list.
    pub prev: *mut Link,
}

impl Link
{
    /// Creates a Link in a detached state (pointers are null).
    pub const fn new_unlinked() -> Self
    {
        Self {
            next: ptr::null_mut(),
            prev: ptr::null_mut(),
        }
    }

    #[inline(always)]
    pub fn is_unlinked(&self) -> bool
    {
        self.next.is_null() && self.prev.is_null()
    }
}

/// A circular intrusive doubly linked list.
pub struct DList<T>
{
    /// Sentinel head node anchoring the circular list.
    head: Link,
    _marker: PhantomData<T>,
}

impl<T> DList<T>
{
    /// Creates a new list instance.
    /// Note: You MUST call .init() after this if the list is moved to its final location.
    pub const fn new_uninit() -> Self
    {
        Self {
            head: Link::new_unlinked(),
            _marker: PhantomData,
        }
    }

    /// Linux-style INIT_LIST_HEAD.
    /// Anchors the circular pointers to the current memory address of the list.
    pub fn init(&mut self)
    {
        let head_ptr = &mut self.head as *mut Link;
        unsafe {
            (*head_ptr).next = head_ptr;
            (*head_ptr).prev = head_ptr;
        }
    }

    /// Returns true if the sentinel points to itself or if the list is uninitialized (null pointers).
    #[inline(always)]
    pub fn is_empty(&self) -> bool
    {
        let head_ptr = &self.head as *const Link;
        self.head.next.is_null() || std::ptr::eq(self.head.next, head_ptr)
    }

    /// Internal helper: Connects a new node between two known nodes.
    #[inline(always)]
    /// # Safety
    ///
    /// The caller must ensure `new_link`, `prev`, and `next` are valid pointers.
    unsafe fn __list_add(&mut self, new_link: *mut Link, prev: *mut Link, next: *mut Link)
    {
        debug_assert_list_inited!(self);
        unsafe {
            (*next).prev = new_link;
            (*new_link).next = next;
            (*new_link).prev = prev;
            (*prev).next = new_link;
        }
    }

    /// Adds an element to the end of the list
    #[inline(always)]
    /// # Safety
    ///
    /// The caller must ensure that `node_link` is a valid, non-null pointer
    /// to a `Link` that is currently unlinked (unless unchecked).
    pub unsafe fn push_back(&mut self, node_link: *mut Link)
    {
        debug_assert_list_inited!(self);

        let head_ptr = &mut self.head as *mut Link;

        debug_assert!(!node_link.is_null());
        debug_assert!(unsafe { (*node_link).is_unlinked() }, "double insert");

        debug_assert!(node_link != head_ptr, "attempted to insert sentinel");

        unsafe {
            self.__list_add(node_link, (*head_ptr).prev, head_ptr);
        }
    }

    /// Adds an element to the end of the list without checking whether it is
    /// already linked. Callers must guarantee queue membership independently.
    #[inline(always)]
    /// # Safety
    ///
    /// The caller must ensure that `node_link` is valid, non-null, and not
    /// currently linked into any list.
    pub unsafe fn push_back_unchecked(&mut self, node_link: *mut Link)
    {
        debug_assert_list_inited!(self);

        let head_ptr = &mut self.head as *mut Link;

        debug_assert!(!node_link.is_null());
        debug_assert!(node_link != head_ptr, "attempted to insert sentinel");

        unsafe {
            self.__list_add(node_link, (*head_ptr).prev, head_ptr);
        }
    }

    /// Adds an element to the front of the list
    #[inline(always)]
    /// # Safety
    ///
    /// The caller must ensure that `node_link` is a valid, non-null pointer
    /// to a `Link` that is currently unlinked (unless unchecked).
    pub unsafe fn push_front(&mut self, node_link: *mut Link)
    {
        debug_assert_list_inited!(self);

        let head_ptr = &mut self.head as *mut Link;

        debug_assert!(!node_link.is_null());
        debug_assert!(unsafe { (*node_link).is_unlinked() }, "double insert");

        debug_assert!(node_link != head_ptr, "attempted to insert sentinel");

        unsafe {
            self.__list_add(node_link, head_ptr, (*head_ptr).next);
        }
    }

    /// Adds an element to the front of the list without checking if it was previously unlinked.
    /// This is an optimization for memory pools where nodes are known to be detached
    /// without explicitly nullifying their pointers.
    #[inline(always)]
    /// # Safety
    ///
    /// The caller must ensure that `node_link` is a valid, non-null pointer
    /// to a `Link` that is currently unlinked (unless unchecked).
    pub unsafe fn push_front_unchecked(&mut self, node_link: *mut Link)
    {
        debug_assert_list_inited!(self);

        let head_ptr = &mut self.head as *mut Link;

        debug_assert!(!node_link.is_null());
        debug_assert!(node_link != head_ptr, "attempted to insert sentinel");

        unsafe {
            self.__list_add(node_link, head_ptr, (*head_ptr).next);
        }
    }

    /// Appends all nodes from `other` to the back of `self` in O(1).
    #[inline(always)]
    pub fn append_back(&mut self, other: &mut Self)
    {
        debug_assert_list_inited!(self);
        debug_assert_list_inited!(other);

        if other.is_empty()
        {
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

    /// Removes a specific node from the list
    #[inline(always)]
    /// # Safety
    ///
    /// The caller must ensure that `node_link` is a valid pointer to a `Link`
    /// that is currently part of this specific list.
    pub unsafe fn remove(&mut self, node_link: *mut Link)
    {
        if node_link.is_null()
        {
            return;
        }

        debug_assert_list_inited!(self);

        let head_ptr = &self.head as *const Link as *mut Link;
        if node_link == head_ptr
        {
            return;
        }

        debug_assert!(
            unsafe { !(*node_link).next.is_null() && !(*node_link).prev.is_null() },
            "remove on unlinked node"
        );

        unsafe {
            let next = (*node_link).next;
            let prev = (*node_link).prev;

            // Do nothing if the node is already unlinked
            if next.is_null() || prev.is_null()
            {
                return;
            }

            (*next).prev = prev;
            (*prev).next = next;

            // Clear pointers to mark as unlinked.
            (*node_link).next = ptr::null_mut();
            (*node_link).prev = ptr::null_mut();
        }
    }

    /// Peeks at the first element
    #[inline(always)]
    pub fn front(&self, offset: usize) -> Option<*mut T>
    {
        debug_assert_list_inited!(self);

        if self.is_empty()
        {
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

    /// Removes and returns the first element
    #[inline(always)]
    /// # Safety
    ///
    /// The caller must ensure that the `offset` correctly represents the byte
    /// distance from the start of the container `T` to the `Link` field.
    pub unsafe fn pop_front(&mut self, offset: usize) -> Option<*mut T>
    {
        debug_assert_list_inited!(self);

        if self.is_empty()
        {
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
    pub fn cursor_mut(&mut self) -> CursorMut<'_, T>
    {
        debug_assert_list_inited!(self);

        let head_ptr = &mut self.head as *mut Link;
        CursorMut {
            current: unsafe { (*head_ptr).next },
            head: head_ptr,
            list: self,
        }
    }

    /// Returns a backward-walking iterator.
    pub fn cursor_back_mut(&mut self) -> CursorBackMut<'_, T>
    {
        debug_assert_list_inited!(self);

        let head_ptr = &mut self.head as *mut Link;
        CursorBackMut {
            current: unsafe { (*head_ptr).prev },
            head: head_ptr,
            _list: self,
        }
    }

    /// Splices the contents of `other` into the back of `self`.
    /// After this operation, `other` will be empty.
    /// This is an O(1) operation regardless of the number of elements.
    #[inline(always)]
    /// # Safety
    ///
    /// The caller must ensure that `other` is a valid, initialized list.
    pub unsafe fn splice_back(&mut self, other: &mut DList<T>)
    {
        debug_assert_list_inited!(self);
        debug_assert_list_inited!(other);

        if other.is_empty()
        {
            return;
        }

        unsafe {
            let self_head = &mut self.head as *mut Link;
            let other_head = &mut other.head as *mut Link;

            let self_last = (*self_head).prev;
            let other_first = (*other_head).next;
            let other_last = (*other_head).prev;

            // 1. Connect the end of self to the start of other
            (*self_last).next = other_first;
            (*other_first).prev = self_last;

            // 2. Connect the end of other back to the head of self
            (*other_last).next = self_head;
            (*self_head).prev = other_last;

            // 3. Reset the other list to an empty state
            other.init();
        }
    }
}

/// Forward Iterator
pub struct CursorMut<'a, T>
{
    current: *mut Link,
    head: *mut Link,
    list: &'a mut DList<T>,
}

impl<'a, T> CursorMut<'a, T>
{
    #[inline(always)]
    /// # Safety
    ///
    /// The caller must ensure that the `offset` correctly represents the byte
    /// distance from the start of the container `T` to the `Link` field.
    pub unsafe fn next_with_offset(&mut self, offset: usize) -> Option<(*mut T, *mut Link)>
    {
        if self.current == self.head
        {
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
    pub unsafe fn remove_link(&mut self, link: *mut Link)
    {
        unsafe {
            self.list.remove(link);
        }
    }
}

/// Backward Iterator
pub struct CursorBackMut<'a, T>
{
    current: *mut Link,
    head: *mut Link,
    _list: &'a mut DList<T>,
}

impl<'a, T> CursorBackMut<'a, T>
{
    #[inline(always)]
    /// # Safety
    ///
    /// The caller must ensure that the `offset` correctly represents the byte
    /// distance from the start of the container `T` to the `Link` field.
    pub unsafe fn prev_with_offset(&mut self, offset: usize) -> Option<(*mut T, *mut Link)>
    {
        if self.current == self.head
        {
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
    pub unsafe fn remove_link(&mut self, link: *mut Link)
    {
        unsafe {
            self._list.remove(link);
        }
    }
}
