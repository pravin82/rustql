use rand::prelude::SliceRandom;
use rand::thread_rng;
use rustql::table::row::{COLUMN_EMAIL_SIZE, COLUMN_USERNAME_SIZE};
use rustql::table::table::{Table, ROWS_PER_PAGE, TABLE_MAX_ROWS};
use std::{fs, io};

const DB_FILE_NAME: &str = "mydb.db";
fn close_test(mut table: Table) {
    unsafe {
        table.db_close();
    }
    fs::remove_file(DB_FILE_NAME);
}

fn start_test() {
    let message = match fs::remove_file(DB_FILE_NAME) {
        Ok(()) => "ok".to_string(),
        Err(e) => e.kind().to_string(),
    };
    println!("{}", message)
}

#[test]
fn it_insert_and_select() {
    start_test();
    let mut table: Table = unsafe { Table::db_open(DB_FILE_NAME) };
    let mut result = Vec::new();
    rustql::run("insert 1 pravin email".to_string(), &mut table, &mut result);
    assert_eq!(result, b"Executed.\n");
    result = Vec::new();
    rustql::run("select".to_string(), &mut table, &mut result);
    assert_eq!(
        result,
        b"1,['p', 'r', 'a', 'v', 'i', 'n'],['e', 'm', 'a', 'i', 'l']\n"
    );
    unsafe {
        rustql::exit_process(table);
    }
    let mut table: Table = unsafe { Table::db_open(DB_FILE_NAME) };
    result = Vec::new();
    rustql::run("select".to_string(), &mut table, &mut result);
    assert_eq!(
        result,
        b"1,['p', 'r', 'a', 'v', 'i', 'n'],['e', 'm', 'a', 'i', 'l']\n"
    );
    close_test(table)
}
#[test]
fn insert_more_than_1_page() {
    start_test();
    let mut table: Table = unsafe { Table::db_open(DB_FILE_NAME) };
    let mut result = Vec::new();
    for i in 1..=ROWS_PER_PAGE + 1 {
        rustql::run(
            format!("insert {} pravin{} email{}", i, i, i),
            &mut table,
            &mut result,
        );
        assert_eq!(result, b"Executed.\n");
        result = Vec::new()
    }
    rustql::run("select".to_string(), &mut table, &mut result);
    assert_eq!(result, get_expected_result(ROWS_PER_PAGE + 1).as_bytes());
    unsafe {
        rustql::exit_process(table);
    }
    result = Vec::new();
    let mut table: Table = unsafe { Table::db_open(DB_FILE_NAME) };
    rustql::run("select".to_string(), &mut table, &mut result);
    assert_eq!(result, get_expected_result(ROWS_PER_PAGE + 1).as_bytes());
    close_test(table)
}

fn get_expected_result(count: u32) -> String {
    let mut result = String::new();
    for i in 1..=count {
        let name = format!("pravin{}", i);
        let email = format!("email{}", i);
        let name_list = name.chars().collect::<Vec<char>>();
        let email_list = email.chars().collect::<Vec<char>>();

        result.push_str(&format!("{},{:?},{:?}\n", i, name_list, email_list));
    }
    result
}

#[test]
fn test_table_full() {
    start_test();
    let mut table: Table = unsafe { Table::db_open(DB_FILE_NAME) };
    let mut result = Vec::new();

    for i in 1..=TABLE_MAX_ROWS + 1 {
        rustql::run(
            format!("insert {} pravin{} email{}", i, i, i),
            &mut table,
            &mut result,
        );
        if i < TABLE_MAX_ROWS {
            assert_eq!(result, b"Executed.\n")
        } else {
            assert_eq!(result, b"Table is full\n")
        }
        result = Vec::new()
    }
    close_test(table)
}

#[test]
fn max_string_length_insert() {
    start_test();
    let mut table: Table = unsafe { Table::db_open(DB_FILE_NAME) };
    let mut result = Vec::new();
    let username: String = ['a'; COLUMN_USERNAME_SIZE].iter().collect();
    let email: String = ['b'; COLUMN_EMAIL_SIZE].iter().collect();
    rustql::run(
        format!("insert 1 {} {}", username, email),
        &mut table,
        &mut result,
    );
    assert_eq!(result, b"Executed.\n");
    result = Vec::new();
    rustql::run("select".to_string(), &mut table, &mut result);
    let printed_username = format!("[{}]", repeat_character("a", COLUMN_USERNAME_SIZE));
    let printed_email = format!("[{}]", repeat_character("b", COLUMN_EMAIL_SIZE));
    let expected_result = format!("1,{},{}\n", printed_username, printed_email);
    assert_eq!(result, expected_result.as_bytes());
    close_test(table)
}

#[test]
fn test_too_long_string() {
    start_test();
    let mut table: Table = unsafe { Table::db_open(DB_FILE_NAME) };
    let mut result = Vec::new();
    let username: String = ['a'; COLUMN_USERNAME_SIZE + 1].iter().collect();
    let email: String = ['b'; 10].iter().collect();
    rustql::run(
        format!("insert 1 {} {}", username, email),
        &mut table,
        &mut result,
    );
    close_test(table);
    assert_eq!(result, b"String is too long.\n");
}

#[test]
fn id_must_be_positive() {
    start_test();
    let mut table: Table = unsafe { Table::db_open(DB_FILE_NAME) };
    let mut result = Vec::new();
    rustql::run(
        "insert -1 pravin email".to_string(),
        &mut table,
        &mut result,
    );
    assert_eq!(result, b"Id must be positive.\n");
    result = Vec::new();
    rustql::run("select".to_string(), &mut table, &mut result);
    assert_eq!(result, b"0,[],[]\n");
    close_test(table)
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
    result.pop(); // Remove the trailing comma
    result
}

fn shuffle_list<T>(list: &mut Vec<T>) {
    let mut rng = thread_rng();
    list.shuffle(&mut rng);
}

#[test]
fn print_tree() {
    start_test();
    let mut table: Table = unsafe { Table::db_open(DB_FILE_NAME) };
    let mut result = Vec::new();
   // let ids = vec![3, 1, 14, 2, 16, 13, 10, 5, 8, 6, 12, 7, 11, 9, 4, 15];
     let mut ids:Vec<u32> = (1..=9).collect();
    //shuffle_list(&mut ids);

    for id in ids {
        rustql::run(
            format!("insert {} pravin{} email{}", id, id, id),
            &mut table,
            &mut result,
        );
        assert_eq!(result, b"Executed.\n");
        result = Vec::new()
    }
    result = Vec::new();
    rustql::run(format!(".btree"), &mut table, &mut io::stdout());
    rustql::run(format!(".btree"), &mut table, &mut result);
    assert_eq!(
        result,
        b"Tree:\n- internal (size 1)\n  - leaf (size 8)\n    - 1\n    - 2\n    - 3\n    - 4\n    - 5\n    - 6\n    - 7\n    - 8\n  - key 8\n  - leaf (size 8)\n    - 9\n    - 10\n    - 11\n    - 12\n    - 13\n    - 14\n    - 15\n    - 16\n"
    );
    result = Vec::new();
    rustql::run(
        format!("insert {} pravin{} email{}", 17, 17, 17),
        &mut table,
        &mut result,
    );
    assert_eq!(result, b"Executed.\n");

    close_test(table)
}

#[test]
fn test_duplicate_keys() {
    start_test();
    let mut table: Table = unsafe { Table::db_open(DB_FILE_NAME) };
    let mut result = Vec::new();
    rustql::run(format!("insert 1 pravin1 email1"), &mut table, &mut result);
    assert_eq!(result, b"Executed.\n");
    result = Vec::new();
    rustql::run(format!("insert 1 pravin2 email2"), &mut table, &mut result);
    assert_eq!(result, b"Error:Duplicate key\n")
}
