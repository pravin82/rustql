use rustql;

#[test]
fn it_insert_and_select() {
    let mut table = rustql::Table::new_table();
    let mut result = Vec::new();
    rustql::run("insert 1 pravin email".to_string(), &mut table,&mut result);
    assert_eq!(result, b"Executed.\n");
    result = Vec::new();
    rustql::run("select".to_string(), &mut table,&mut result);
    assert_eq!(result, b"1,['p', 'r', 'a', 'v', 'i', 'n'],['e', 'm', 'a', 'i', 'l']\n");


}