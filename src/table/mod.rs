use std::mem;
use row::ROW_SIZE;

pub mod row;

const TABLE_MAX_PAGES: u32 = 100;
const  PAGE_SIZE: u32 = 4096;

const ROWS_PER_PAGE:u32 = (PAGE_SIZE as usize/ROW_SIZE) as u32;
pub const TABLE_MAX_ROWS:u32 = ROWS_PER_PAGE*TABLE_MAX_PAGES;
pub struct Table{
  pub num_rows: u32,
   pages:Box<[Option<*mut u8>; TABLE_MAX_PAGES as usize]>

}


impl Table{

    pub fn new_table() -> Table{

        let pages: Box<[Option<*mut u8>; TABLE_MAX_PAGES as usize]> = Box::new([None; TABLE_MAX_PAGES as usize]);
        Table{
            num_rows: 0,
            pages,
        }
    }

    pub fn free_table(self){
       for page in self.pages.into_iter(){
           drop(page)
       }
        drop(self)
    }
   //row_num starts from 0
   pub unsafe fn row_slot(&mut self, row_num:u32) -> *mut u8{
       let page_num = row_num/ROWS_PER_PAGE;
       let mut page_ptr = self.pages[page_num as usize];
       let row_offset =  (row_num as usize % ROWS_PER_PAGE as usize)*ROW_SIZE;
       let byte_offset = row_offset * ROW_SIZE;
      if(page_ptr.is_none()){
          let mut new_page:Box<[u8;ROW_SIZE]> =Box::new([0u8;ROW_SIZE]);
          page_ptr = Some(new_page.as_mut_ptr());
          mem::forget(new_page);
          self.pages[page_num as usize] = page_ptr
      }
      let row_ptr  = page_ptr.unwrap().offset(byte_offset as isize);
      row_ptr
   }



}


