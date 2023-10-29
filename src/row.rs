use std::{mem, ptr};

const COLUMN_USERNAME_SIZE: usize = 32;
const COLUMN_EMAIL_SIZE:usize = 255;

macro_rules! field_size {
    ($t:ident :: $field:ident) => {{
        let m = core::mem::MaybeUninit::<$t>::uninit();
        // According to https://doc.rust-lang.org/stable/std/ptr/macro.addr_of_mut.html#examples,
        // you can dereference an uninitialized MaybeUninit pointer in addr_of!
        // Raw pointer deref in const contexts is stabilized in 1.58:
        // https://github.com/rust-lang/rust/pull/89551
        let p = unsafe {
            core::ptr::addr_of!((*(&m as *const _ as *const $t)).$field)
        };

        const fn size_of_raw<T>(_: *const T) -> usize {
            core::mem::size_of::<T>()
        }
        size_of_raw(p)
    }};
}
pub struct Row{
    id: u32,
    username:[char;COLUMN_USERNAME_SIZE],
    email:[char;COLUMN_EMAIL_SIZE]
}

const ID_SIZE: usize = field_size!(Row::id);
const USERNAME_SIZE:usize =  field_size!(Row::username);
const EMAIL_SIZE:usize =  field_size!(Row::email);
const ID_OFFSET:usize = 0;
const USERNAME_OFFSET:usize = ID_OFFSET+ID_SIZE;
const EMAIL_OFFSET:usize = USERNAME_OFFSET+USERNAME_SIZE;
const ROW_SIZE:usize = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;





fn serialize_row(source: &Row, destination: &mut [u8]) {
    if destination.len() < ROW_SIZE {
        panic!("Destination buffer too small for serialization.");
    }

    unsafe {
        let destination_ptr = destination.as_mut_ptr();
        let x = u32::to_be_bytes(source.id);
        for i in 0..4{
            let id_byte = x[i];
            ptr::copy(&id_byte, destination_ptr.add(ID_OFFSET + i), 1);
        }

        // Convert the username characters to bytes before copying
        let username_str: String = source.username.iter().collect();
        let username_bytes: Vec<u8> = username_str.as_bytes().to_vec();
        let email_str:String = source.email.iter().collect();
        let email_bytes:Vec<u8> = email_str.as_bytes().to_vec();

        for i in 0..username_bytes.len() {
            let byte = username_bytes[i];
            ptr::copy(&byte, destination_ptr.add(USERNAME_OFFSET + i), 1);
        }
        for i in 0..email_bytes.len() {
            let byte = email_bytes[i];
            ptr::copy(&byte, destination_ptr.add(EMAIL_OFFSET + i), 1);
        }

    }
}


fn deserialize_row(source: &[u8])->Row{
    let id_bytes: [u8; ID_SIZE] = source[ID_OFFSET..ID_OFFSET + ID_SIZE]
        .try_into()
        .expect("Failed to deserialize ID");
    let username_bytes:[u8;USERNAME_SIZE] = source[USERNAME_OFFSET..USERNAME_OFFSET+USERNAME_SIZE]
       .try_into()
       .expect("Failed to desrialize username");
    let email_bytes:[u8;EMAIL_SIZE] = source[EMAIL_OFFSET..EMAIL_OFFSET+EMAIL_SIZE]
        .try_into()
        .expect("Failed to deserialize email");
    let id: u32 = u32::from_be_bytes(id_bytes);
    let username:[char;COLUMN_USERNAME_SIZE]= String::from_utf8(Vec::from(username_bytes)).expect("Invalid UTF8").chars()
         .collect::<Vec<char>>().try_into().expect("Failed to desrialize username");
    let email:[char;COLUMN_EMAIL_SIZE]= String::from_utf8(Vec::from(email_bytes)).expect("Invalid UTF8").chars()
        .collect::<Vec<char>>().try_into().expect("Failed to desrialize email");
    Row{
        id,
        username,
        email
    }




}