use row::ROW_SIZE;

pub mod row;

const TABLE_MAX_PAGES: usize = 100;
const  PAGE_SIZE: usize = 4096;

const ROWS_PER_PAGE:usize = (PAGE_SIZE/ROW_SIZE);
const TABLE_MAX_ROWS:usize = ROWS_PER_PAGE*TABLE_MAX_PAGES;
pub struct Table{
   num_rows: u32,
   pages:[Option<*mut u8>;TABLE_MAX_PAGES]

}


impl Table{
   //row_num starts from 0
   unsafe fn row_slot(&self, row_num:usize) -> *mut u8{
       let page_num = row_num/ROWS_PER_PAGE;
       let mut page_ptr = self.pages[page_num];
       let row_offset =  (row_num % ROWS_PER_PAGE)*ROW_SIZE;
       let byte_offset = row_offset * ROW_SIZE;
      if(page_ptr.is_none()){
          let mut new_page:[u8;ROW_SIZE] = [0u8;ROW_SIZE];
          page_ptr = Some(new_page.as_mut_ptr());

      }
      let row_ptr  = page_ptr.unwrap().offset(byte_offset as isize);
      row_ptr
   }

}


