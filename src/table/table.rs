use crate::pager::pager::{Pager, TABLE_MAX_PAGES};
use crate::table::row::ROW_SIZE;
use libm::ceil;
use std::mem;
use crate::node::node::Node;

pub const PAGE_SIZE: usize = 4096;

pub const ROWS_PER_PAGE: u32 = (PAGE_SIZE as usize / ROW_SIZE) as u32;
pub const TABLE_MAX_ROWS: u32 = ROWS_PER_PAGE * TABLE_MAX_PAGES;

pub struct Table {
    pub num_rows: u32,
    pub pager: Pager,
    pub root_page_num: u32,
}

impl Table {
    pub unsafe fn db_open(filename: &str) -> Table {
        let mut pager = Pager::open(filename).unwrap();
        let num_rows = (pager.file_length) / ROW_SIZE as u32;
        if (pager.num_pages == 0) {
            // New database file. Initialize page 0 as leaf node.
            let root_node = pager.get_page(0).unwrap();
            Node::initialize_leaf_node(root_node)
        }
        Table {
            num_rows,
            pager,
            root_page_num: 0,
        }
    }

    pub unsafe fn db_close(mut self) {
        self.flush_pages();
        self.free_pages();
        drop(&self.pager);
        drop(self);
    }

    unsafe fn flush_pages(&mut self) {
        for i in 0..self.pager.num_pages {
            self.pager.flush_page(i as usize);
        }
        let extra_rows = (self.num_rows % ROWS_PER_PAGE);
        if (extra_rows == 0) {
            return;
        }
    }

    pub fn free_pages(&mut self) {
        for i in 0..self.pager.pages.len() {
            drop(self.pager.pages[i]);
            self.pager.pages[i] = None;
        }
    }
    //row_num starts from 0
    pub unsafe fn row_slot(&mut self, row_num: u32) -> *mut u8 {
        let page_num = row_num / ROWS_PER_PAGE;
        let mut page_ptr = self.pager.get_page(page_num).unwrap();
        let row_offset = (row_num as usize % ROWS_PER_PAGE as usize) * ROW_SIZE;
        page_ptr.add(row_offset)
    }
}
