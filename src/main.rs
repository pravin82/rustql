mod statement;
mod row;
mod table;

use std::io;
use std::io::SeekFrom::Start;
use std::io::Write;
use std::os::macos::raw::stat;
use std::process::exit;
use std::str::FromStr;
use crate::statement::StatementType;

fn main() {
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
         execute_statement(command)
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

fn execute_statement(command:&str){
 if let Ok(statement) =  get_command_type(command) {
    match statement {
       StatementType::INSERT => println!("Insert will be executed"),
       StatementType::UPDATE => println!("Update statement will be exeucted"),
       StatementType::SELECT => println!("select will be executed")
    }
 }
   else {
      println!("wrong command")
   }
}

fn get_command_type(command:&str) -> Result<StatementType, ()>{
   let chunks: Vec<&str> =  command.split(" ").collect();
   let f = StatementType::from_str(chunks[0]);
   return f
}




