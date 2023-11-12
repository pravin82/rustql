use crate::table::row::ROW_SIZE;
use crate::table::table::PAGE_SIZE;
use libm::ceil;
use std::fs::{File, OpenOptions};
use std::io::{Error, ErrorKind, Read, Seek, SeekFrom, Write};
use std::ptr::{null, slice_from_raw_parts};
use std::{io, mem};

pub const TABLE_MAX_PAGES: u32 = 100;

pub struct Pager {
    pub file: File,
    pub file_length: u32,
    pub pages: [Option<*mut u8>; TABLE_MAX_PAGES as usize],
    pub num_pages: u32,
}

impl Pager {
    pub fn open(filename: &str) -> io::Result<Self> {
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true);
        let file = options.open(filename)?;
        let metadata = file.metadata()?;
        let file_length = metadata.len() as u32;
        let num_pages = file_length / PAGE_SIZE as u32;
        let pages = [None; TABLE_MAX_PAGES as usize];
        if (file_length % PAGE_SIZE as u32 != 0) {
            return Err(Error::new(
                ErrorKind::Other,
                "Db file is not whole no of page size",
            ));
        }
        Ok(Pager {
            file,
            file_length,
            pages,
            num_pages,
        })
    }

    pub fn get_page(&mut self, page_num: u32) -> Result<*mut u8, Error> {
        if (page_num > TABLE_MAX_PAGES) {
            return Err(Error::new(
                ErrorKind::Other,
                "Tried to fetch page number out of bounds",
            ));
        }
        let mut page_ptr = self.pages[page_num as usize];
        if (page_ptr.is_none()) {
            let page_offset = page_num * PAGE_SIZE as u32;
            self.file.seek(SeekFrom::Start(page_offset as u64));
            let mut buffer: Vec<u8> = vec![0; PAGE_SIZE];
            let mut total_bytes_read = 0;

            while total_bytes_read < PAGE_SIZE {
                let bytes_read = self
                    .file
                    .read(&mut buffer[total_bytes_read..])
                    .expect("Failed to read from file");
                if bytes_read == 0 {
                    // End of file reached
                    break;
                }
                total_bytes_read += bytes_read;
            }
            page_ptr = Some(buffer.as_mut_ptr());
            mem::forget(buffer);
            self.pages[page_num as usize] = page_ptr;
            if (page_num >= self.num_pages) {
                self.num_pages = page_num + 1
            }
        }
        Ok(page_ptr.unwrap())
    }

    pub unsafe fn flush_page(&mut self, page_num: usize) -> Result<String, Error> {
        let page_ptr = self.get_page(page_num as u32).unwrap();
        let start_offset = page_num * PAGE_SIZE;
        self.file.seek(SeekFrom::Start(start_offset as u64));
        let data = slice_from_raw_parts(page_ptr, PAGE_SIZE);
        self.file.write(&*data);
        Ok("SUCCESS".parse().unwrap())
    }
}
