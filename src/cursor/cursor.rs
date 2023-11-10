use crate::table::row::ROW_SIZE;
use crate::table::table::{Table, ROWS_PER_PAGE};

pub struct Cursor {
    row_num: u32,
    pub end_of_table: bool,
}

impl Cursor {
    pub fn table_start(table: &Table) -> Cursor {
        let num_rows = table.num_rows;
        Cursor {
            row_num: 0,
            end_of_table: (num_rows == 0),
        }
    }

    pub fn table_end(table: &Table) -> Cursor {
        let num_rows = table.num_rows;
        Cursor {
            row_num: num_rows,
            end_of_table: true,
        }
    }

    pub unsafe fn cursor_value(&mut self, table: &mut Table) -> *mut u8 {
        let row_num = self.row_num;
        let page_num = row_num / ROWS_PER_PAGE;
        let mut page_ptr = table.pager.get_page(page_num).unwrap();
        let row_offset = (row_num as usize % ROWS_PER_PAGE as usize) * ROW_SIZE;
        page_ptr.add(row_offset)
    }

    pub fn advance_cursor(&mut self, table: &Table) {
        self.row_num += 1;
        if (self.row_num >= table.num_rows) {
            self.end_of_table = true
        }
    }
}
