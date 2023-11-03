mod statement;
mod table;

use std::{io, ptr, slice};
use std::io::SeekFrom::Start;
use std::io::{Error, ErrorKind, Write};
use std::os::macos::raw::stat;
use std::process::exit;
use std::str::FromStr;
use crate::statement::{Statement, StatementType};
use crate::table::{Table, TABLE_MAX_ROWS};
use crate::table::row::{Row, ROW_SIZE};


 fn main() {
    let mut table = Table::new_table();
   while(true){
      print_prompt();
      let mut line = String::new();
      let command_string = std::io::stdin().read_line(&mut line).unwrap();
      let command = line.trim();
      if(command.starts_with(".")){
         execute_meta_command(command)  ;
         continue
      }
      else {
        unsafe {
           execute_statement(command,&mut table)
        }
      }


   }
    
}

fn print_prompt(){
   print!("db > ");
   io::stdout().flush();

}

fn execute_meta_command(command: &str){
   if(command == ".exit"){
      exit(0)
   }
   else {
      println!("Unrecognised command '{}'", command)
   }

}

unsafe fn execute_statement(command:&str, table:&mut Table){
 let statement = Statement::prepare_statement(command);
    match statement.statement_type {
       StatementType::INSERT =>{
         execute_insert(statement,table).expect("Insert failed");
             ()
       } 
       StatementType::UPDATE => println!("Update statement will be exeucted"),
       StatementType::SELECT => {
          execute_select(table);
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

unsafe  fn execute_select(table:&mut Table)->Result<&'static str, Error>{
   for i in 0..table.num_rows{
      let row_ptr = table.row_slot(i);
      let mut bytes:[u8;ROW_SIZE] = [0u8;ROW_SIZE];
      for  i in 0..ROW_SIZE{
         bytes[i] = ptr::read(row_ptr.offset(i as isize));
      }
      let deserialized_row = Row::deserialize_row(&bytes);
      Row::print_row(deserialized_row)

   }
   Ok("SUCCESS")

}

fn get_command_type(command:&str) -> Result<StatementType, ()>{
   let chunks: Vec<&str> =  command.split(" ").collect();
   let f = StatementType::from_str(chunks[0]);
   return f
}




