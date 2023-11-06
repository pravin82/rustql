use crate::table::table::TABLE_MAX_PAGES;

struct Pager {
file_descriptor:usize,
file_length:u32,
 pages:Box<[Option<*mut u8>; TABLE_MAX_PAGES as usize]>
}