use std::{mem, ptr};

pub const COLUMN_USERNAME_SIZE: usize = 32;
pub const COLUMN_EMAIL_SIZE:usize = 255;

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
   pub id: u32,
   pub username:[char;COLUMN_USERNAME_SIZE],
   pub email:[char;COLUMN_EMAIL_SIZE]
}

const ID_SIZE: usize = field_size!(Row::id);
const USERNAME_SIZE:usize =  field_size!(Row::username);
const EMAIL_SIZE:usize =  field_size!(Row::email);
const ID_OFFSET:usize = 0;
const USERNAME_OFFSET:usize = (ID_OFFSET + ID_SIZE);
const EMAIL_OFFSET:usize = USERNAME_OFFSET+USERNAME_SIZE;
pub(crate) const ROW_SIZE:usize = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;





impl Row {
    pub fn serialize_row(&self, destination_ptr: *mut u8) {
        // if destination.len() < ROW_SIZE {
        //     panic!("Destination buffer too small for serialization.");
        // }

        unsafe {
            let x = u32::to_be_bytes(self.id);
            for i in 0..4 {
                let id_byte = x[i];
                ptr::copy(&id_byte, destination_ptr.offset((ID_OFFSET + i) as isize), 1);
            }

            // Convert the username characters to bytes before copying
            let username_bytes: Vec<u8> = char_array_to_bytes1(&self.username);
            let email_bytes: Vec<u8> = char_array_to_bytes1(&self.email);
            //email_str.as_bytes().to_vec();

            for i in 0..USERNAME_SIZE {
                let byte = username_bytes[i];
                ptr::copy(&byte, destination_ptr.offset((USERNAME_OFFSET + i) as isize), 1);
            }
            for i in 0..EMAIL_SIZE {
                let byte = email_bytes[i];
                ptr::copy(&byte, destination_ptr.offset((EMAIL_OFFSET + i) as isize), 1);
            }
        }
    }
    pub fn deserialize_row(source: &[u8]) -> Row {
        let id_bytes: [u8; ID_SIZE] = source[ID_OFFSET..ID_OFFSET + ID_SIZE]
            .try_into()
            .expect("Failed to deserialize ID");
        let username_bytes: [u8; USERNAME_SIZE] = source[USERNAME_OFFSET..USERNAME_OFFSET + USERNAME_SIZE]
            .try_into()
            .expect("Failed to desrialize username");
        let email_bytes: [u8; EMAIL_SIZE] = source[EMAIL_OFFSET..EMAIL_OFFSET + EMAIL_SIZE]
            .try_into()
            .expect("Failed to deserialize email");
        let id: u32 = u32::from_be_bytes(id_bytes);
        let username: [char; COLUMN_USERNAME_SIZE] = bytes_to_char_array(&username_bytes)
            .try_into().expect("Failed to desrialize username");
        let email: [char; COLUMN_EMAIL_SIZE] = bytes_to_char_array(&email_bytes).try_into().expect("Failed to desrialize email");
        Row {
            id,
            username,
            email
        }
    }
    pub fn print_row(row: Row) {

        println!("{},{:?},{:?}",
                 row.id,
                 remove_default_chars(&row.username),
                 remove_default_chars(&row.email)
        )
    }


}

fn char_to_bytes(c: char) -> [u8; 4] {
    let mut bytes = [0; 4];
    c.encode_utf8(&mut bytes);
    bytes
}

fn char_array_to_bytes(char_array: &[char]) -> Vec<u8> {
    char_array.iter().map(|&c| char_to_bytes(c)).collect::<Vec<[u8;4]>>()
        .into_iter().flatten().collect::<Vec<u8>>()
}

fn char_array_to_bytes1(char_array: &[char]) -> Vec<u8> {
    let mut result = Vec::with_capacity(char_array.len() * 4);
    for &c in char_array {
        result.extend_from_slice(&char_to_bytes(c));
    }
    result
}

fn bytes_to_char_array(bytes_array: &[u8]) -> Vec<char> {
    bytes_array
        .chunks(4)
        .map(|chunk| {
            let mut buf = [0; 4];
            buf.copy_from_slice(chunk);
            let s = std::str::from_utf8(&buf).expect("Invalid UTF-8");
            s.chars().next().expect("Invalid char")
        })
        .collect()
}

fn remove_default_chars(chars:&[char]) -> Vec<char>{
    chars.iter().filter(|&c| *c != '\0').cloned().collect()
}











