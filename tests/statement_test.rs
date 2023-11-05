use rustql;
use rustql::{Table, TABLE_MAX_ROWS};

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

