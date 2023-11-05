
use std::{io};
use std::io::{ Write};
use rustql;


 fn main() {
    let mut table = rustql::Table::new_table();
   loop{
      print_prompt();
      let mut command = String::new();
       io::stdin().read_line(&mut command).unwrap();
      rustql::run(command, &mut table, &mut io::stdout());
   }
    
}

fn print_prompt(){
   print!("db > ");
   io::stdout().flush();

}









