use std::fmt::format;
use std::fs;
use std::io::{ErrorKind, Write};
use std::process::Command;
use rustql::table::table::{ROWS_PER_PAGE, Table, TABLE_MAX_ROWS};
use rustql::table::row::{COLUMN_EMAIL_SIZE, COLUMN_USERNAME_SIZE};
const DB_FILE_NAME: &str = "mydb.db";
fn close_test(mut table: Table){
    unsafe { table.db_close(); }
    fs::remove_file(DB_FILE_NAME);

}

fn start_test(){
    let message = match fs::remove_file(DB_FILE_NAME) {
        Ok(()) => "ok".to_string(),
        Err(e)=> {
            e.kind().to_string()
        }
    };
    println!("{}", message)
}

#[test]
fn it_insert_and_select() {
    start_test();
    let   mut table: Table = Table::db_open(DB_FILE_NAME);
    let mut result = Vec::new();
    rustql::run("insert 1 pravin email".to_string(), &mut table,&mut result);
    assert_eq!(result, b"Executed.\n");
    result = Vec::new();
    rustql::run("select".to_string(), &mut table,&mut result);
    assert_eq!(result, b"1,['p', 'r', 'a', 'v', 'i', 'n'],['e', 'm', 'a', 'i', 'l']\n");
    unsafe { rustql::exit_process (&mut table); }
    result = Vec::new();
    rustql::run("select".to_string(), &mut table,&mut result);
    assert_eq!(result, b"1,['p', 'r', 'a', 'v', 'i', 'n'],['e', 'm', 'a', 'i', 'l']\n");
    close_test(table)

}
#[test]
fn insert_more_than_1_page() {
    start_test();
    let   mut table: Table = Table::db_open(DB_FILE_NAME);
    let mut result = Vec::new();
    for i in 1..=ROWS_PER_PAGE+1{
        rustql::run(format!("insert {} pravin{} email{}",i,i,i), &mut table,&mut result);
        assert_eq!(result, b"Executed.\n" );
        result = Vec::new()
    }
    rustql::run("select".to_string(), &mut table,&mut result);
    assert_eq!(result, get_expected_result(ROWS_PER_PAGE+1).as_bytes());
    result = Vec::new();
    close_test(table)

}

fn get_expected_result(count:u32)->String{
    let mut result = String::new();
    for i in 1..=count{
        let name_list: Vec<char> = vec!['p', 'r', 'a', 'v', 'i', 'n', char::from_digit(i, 10).unwrap()];
        let email_list: Vec<char> = vec!['e', 'm', 'a', 'i', 'l', char::from_digit(i, 10).unwrap()];
        result.push_str(&format!("{},{:?},{:?}\n", i, name_list, email_list));
    }
    result
}



#[test]
fn test_table_full(){
    start_test();
    let   mut table: Table = Table::db_open(DB_FILE_NAME);
    let mut result = Vec::new();

    for i in 0..TABLE_MAX_ROWS+1{
        rustql::run(format!("insert {} pravin{} email{}",i,i,i), &mut table,&mut result);
        if i < TABLE_MAX_ROWS {
            assert_eq!(result, b"Executed.\n" )
        }
        else {
            assert_eq!(result, b"Table is full\n")
        }
        result = Vec::new()
    }
    unsafe { table.db_close(); }
    fs::remove_file(DB_FILE_NAME);

}

#[test]
fn max_string_length_insert(){
    let   mut table: Table = Table::db_open(DB_FILE_NAME);
    let mut result = Vec::new();
    let username:String = ['a'; COLUMN_USERNAME_SIZE].iter().collect();
    let email:String = ['b'; COLUMN_EMAIL_SIZE].iter().collect();
    rustql::run(format!("insert 1 {} {}",username,email), &mut table,&mut result);
    assert_eq!(result, b"Executed.\n");
    result = Vec::new();
    rustql::run("select".to_string(), &mut table,&mut result);
    let printed_username = format!("[{}]", repeat_character("a", COLUMN_USERNAME_SIZE));
    let printed_email = format!("[{}]", repeat_character("b", COLUMN_EMAIL_SIZE));
    let expected_result = format!("1,{},{}\n",printed_username,printed_email);
    assert_eq!(result, expected_result.as_bytes());
    unsafe { table.db_close() ;}
    fs::remove_file(DB_FILE_NAME);

}

#[test]
fn test_too_long_string(){
    let   mut table: Table = Table::db_open(DB_FILE_NAME);
    let mut result = Vec::new();
    let username:String = ['a'; COLUMN_USERNAME_SIZE+1].iter().collect();
    let email:String = ['b'; 10].iter().collect();
    rustql::run(format!("insert 1 {} {}",username,email), &mut table,&mut result);
    unsafe { table.db_close(); }
    fs::remove_file(DB_FILE_NAME);
    assert_eq!(result, b"String is too long.\n");
}

#[test]
fn id_must_be_positive(){
    let mut table: Table = Table::db_open(DB_FILE_NAME);
    let mut result = Vec::new();
    rustql::run("insert -1 pravin email".to_string(), &mut table,&mut result);
    assert_eq!(result, b"Id must be positive.\n");
    result = Vec::new();
    rustql::run("select".to_string(), &mut table,&mut result);
    assert_eq!(result, b"");
    unsafe { table.db_close(); }
    fs::remove_file(DB_FILE_NAME);

}


fn repeat_character(character: &str, count: usize) -> String {
    let mut result = String::new();
    for _ in 0..count {
        result.push('\'');
        result.push_str(character);
        result.push('\'');
        result.push(',');
        result.push(' ');
    }
    result.pop();
    result.pop();// Remove the trailing comma
    result
}



