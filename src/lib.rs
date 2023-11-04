mod table;
mod statement;


pub use crate::table::{Table, TABLE_MAX_ROWS};
use std::{io, ptr};
use std::io::{Error, ErrorKind, Write};
use std::process::exit;
use std::str::FromStr;
use crate::statement::{Statement, StatementType};
use crate::table::row::{Row, ROW_SIZE};


pub fn run(command:String, table: &mut Table,mut writer: impl Write){
    let command = command.trim();
    if(command.is_empty()) {return}
    if(command.starts_with(".")){
        execute_meta_command(command)  ;
        return
    }
    else {
        unsafe {
            execute_statement(command,table,writer)
        }
    }

}



fn execute_meta_command(command: &str){
    if(command == ".exit"){
        exit(0)
    }
    else {
        println!("Unrecognised command '{}'", command)
    }

}

unsafe fn execute_statement(command:&str, table:&mut Table,mut writer: impl Write){
    let statement = Statement::prepare_statement(command);
    match statement.statement_type {
        StatementType::INSERT =>{
            let result =  execute_insert(statement,table).expect("Insert failed");
            if(result == "EXECUTE_SUCCESS"){
               let result =  writeln!(writer,"Executed.");
                ()

            }
        }
        StatementType::UPDATE => {
           let result =  writeln!(writer,"Update statement will be exeucted");
            ()
        }
        StatementType::SELECT => {
            execute_select(table,writer);
            ()

        }
    }

}



unsafe  fn execute_insert(statement:Statement,  table: &mut Table) ->Result<&'static str, Error>{
    let row = statement.row_to_insert;
    if(table.num_rows>= TABLE_MAX_ROWS){
        return Err(Error::new(ErrorKind::Other,"Table is full"));
    }
    let row_slot = table.row_slot(table.num_rows);
    row.serialize_row(row_slot);
    table.num_rows = table.num_rows +1;
    return Ok("EXECUTE_SUCCESS")
}

unsafe  fn execute_select(table:&mut Table, mut writer:  impl Write) ->Result<&'static str, Error>{
    for i in 0..table.num_rows{
        let row_ptr = table.row_slot(i);
        let mut bytes:[u8;ROW_SIZE] = [0u8;ROW_SIZE];
        for  i in 0..ROW_SIZE{
            bytes[i] = ptr::read(row_ptr.offset(i as isize));
        }
        let deserialized_row = Row::deserialize_row(&bytes);
        let result = Row::print_row(deserialized_row,& mut writer);
        ()

    }
    Ok("SUCCESS")

}

fn get_command_type(command:&str) -> Result<StatementType, ()>{
    let chunks: Vec<&str> =  command.split(" ").collect();
    let f = StatementType::from_str(chunks[0]);
    return f
}

