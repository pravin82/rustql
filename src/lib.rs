mod pager;
mod statement;
pub mod table;

use crate::statement::{Statement, StatementType};
use crate::table::row::{Row, ROW_SIZE};
use crate::table::table::{Table, TABLE_MAX_ROWS};
use std::io::{Error, ErrorKind, Write};
use std::ops::Deref;
use std::process::exit;
use std::ptr;

pub fn run(command: String, table: &mut Table, writer: impl Write) {
    let command = command.trim();
    if command.is_empty() {
        return;
    }
    if command.starts_with('.') {
        unsafe {
            execute_meta_command(command);
        }
    } else {
        unsafe { execute_statement(command, table, writer) }
    }
}

unsafe fn execute_meta_command(command: &str) {

        println!("Unrecognised command '{}'", command)

}

pub unsafe fn exit_process( table:  Table) {
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
    if table.num_rows >= TABLE_MAX_ROWS {
        return Err(Error::new(ErrorKind::Other, "Table is full"));
    }
    let row_slot = table.row_slot(table.num_rows);
    row.serialize_row(row_slot);
    table.num_rows += 1;
    Ok("EXECUTE_SUCCESS".parse().unwrap())
}

unsafe fn execute_select(table: &mut Table, mut writer: impl Write) -> Result<&'static str, Error> {
    for i in 0..table.num_rows {
        let row_ptr = table.row_slot(i);
        let mut bytes: [u8; ROW_SIZE] = [0u8; ROW_SIZE];
        for i in 0..ROW_SIZE {
            bytes[i] = ptr::read(row_ptr.add(i));
        }
        let deserialized_row = Row::deserialize_row(&bytes);
        let _result = Row::print_row(deserialized_row, &mut writer);
    }
    Ok("SUCCESS")
}
