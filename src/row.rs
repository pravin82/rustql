
const COLUMN_USERNAME_SIZE: usize = 32;
const COLUMN_EMAIL_SIZE:usize = 255;
pub struct Row{
    id: i32,
    username:[char;COLUMN_USERNAME_SIZE],
    email:[char;COLUMN_EMAIL_SIZE]
}