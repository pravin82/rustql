mod cursor;
mod pager;
mod statement;
pub mod table;
mod node;

use crate::cursor::cursor::Cursor;
use crate::statement::{Statement, StatementType};
use crate::table::row::{Row, ROW_SIZE};
use crate::table::table::{Table, TABLE_MAX_ROWS};
use std::io::{Error, ErrorKind, Write};
use std::ops::Deref;

use std::process::exit;
use std::ptr;
use crate::node::get_leaf_node_num_cells;
use crate::node::node::{find_key_in_leaf_node, leaf_node_insert, print_leaf_node};

pub fn run(command: String, table: &mut Table, writer: impl Write) {
    let command = command.trim();
    if command.is_empty() {
        return;
    }
    if command.starts_with('.') {
        unsafe {
            execute_meta_command(command,table,writer);
        }
    } else {
        unsafe { execute_statement(command, table, writer) }
    }
}

unsafe fn execute_meta_command(command: &str, table: &mut Table,mut writer: impl Write) {
    if(command == ".btree"){
        writeln!(writer,"Tree:");
        print_leaf_node(table.pager.get_page(0).unwrap(),writer)

    }
    else {
        println!("Unrecognised command '{}'", command)
    }

}

pub unsafe fn exit_process(table: Table) {
    table.db_close();
}

unsafe fn execute_statement(command: &str, table: &mut Table, mut writer: impl Write) {
    let statement_result = Statement::prepare_statement(command);
    match statement_result {
        Ok(statement) => match statement.statement_type {
            StatementType::INSERT => {
                let insert_resp_result = execute_insert(statement, table);
                let insert_resp = match insert_resp_result {
                    Ok(resp) => resp,
                    Err(e) => e.to_string(),
                };
                if insert_resp == "EXECUTE_SUCCESS" {
                    writeln!(writer, "Executed.");
                } else {
                    writeln!(writer, "{}", insert_resp);
                }
            }
            StatementType::UPDATE => {
                let _result = writeln!(writer, "Update statement will be executed");
            }
            StatementType::SELECT => {
                execute_select(table, writer);
            }
        },
        Err(error) => {
            // Handle the error and print a message
            writeln!(writer, "{}", error.to_string());
        }
    }
}

unsafe fn execute_insert(statement: Statement, table: &mut Table) -> Result<String, Error> {
    let row = statement.row_to_insert;
    let  cursor = Cursor::find_key(table,row.id);
    if table.num_rows >= TABLE_MAX_ROWS {
        return Err(Error::new(ErrorKind::Other, "Table is full"));
    }
    leaf_node_insert(cursor,row.id,row,table)
}

unsafe fn execute_select(table: &mut Table, mut writer: impl Write) -> Result<&'static str, Error> {
    let mut cursor = Cursor::table_start(table);
    while (!cursor.end_of_table) {
        let row_ptr = cursor.cursor_value(table);
        let mut bytes: [u8; ROW_SIZE] = [0u8; ROW_SIZE];
        for i in 0..ROW_SIZE {
            bytes[i] = ptr::read(row_ptr.add(i));
        }
        let deserialized_row = Row::deserialize_row(&bytes);
        let _result = Row::print_row(deserialized_row, &mut writer);
        cursor.advance_cursor(table)
    }
    Ok("SUCCESS")
}
