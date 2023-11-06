use std::fmt::format;
use rustql::table::table::{Table, TABLE_MAX_ROWS};
use rustql::table::row::{COLUMN_EMAIL_SIZE, COLUMN_USERNAME_SIZE};


#[test]
fn it_insert_and_select() {
    let   mut table: Table = Table::new_table();
    let mut result = Vec::new();
    rustql::run("insert 1 pravin email".to_string(), &mut table,&mut result);
    assert_eq!(result, b"Executed.\n");
    result = Vec::new();
    rustql::run("select".to_string(), &mut table,&mut result);
    assert_eq!(result, b"1,['p', 'r', 'a', 'v', 'i', 'n'],['e', 'm', 'a', 'i', 'l']\n");
    table.free_table();

}
#[test]
fn test_table_full(){
    let   mut table: Table = Table::new_table();
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
    table.free_table()

}

#[test]
fn max_string_length_insert(){
    let   mut table: Table = Table::new_table();
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
    table.free_table()

}

#[test]
fn test_too_long_string(){
    let   mut table: Table = Table::new_table();
    let mut result = Vec::new();
    let username:String = ['a'; COLUMN_USERNAME_SIZE+1].iter().collect();
    let email:String = ['b'; 10].iter().collect();
    rustql::run(format!("insert 1 {} {}",username,email), &mut table,&mut result);
    table.free_table();
    assert_eq!(result, b"String is too long.\n");
}

#[test]
fn id_must_be_positive(){
    let mut table: Table = Table::new_table();
    let mut result = Vec::new();
    rustql::run("insert -1 pravin email".to_string(), &mut table,&mut result);
    assert_eq!(result, b"Id must be positive.\n");
    result = Vec::new();
    rustql::run("select".to_string(), &mut table,&mut result);
    assert_eq!(result, b"");
    table.free_table();

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



