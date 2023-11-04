
use std::{io};
use std::io::{ Write};
use rustql;


 fn main() {
    let mut table = rustql::Table::new_table();
   while(true){
      print_prompt();
      let mut line = String::new();
      let command_string = io::stdin().read_line(&mut line).unwrap();
      let command = line.trim();
      rustql::run(command, &mut table);
   }
    
}

fn print_prompt(){
   print!("db > ");
   io::stdout().flush();

}









