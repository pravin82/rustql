
use std::{env, io};
use std::io::{ Write};



 fn main() {

     let args: Vec<String> = env::args().collect();
     if(args.len() < 2) {
         print!("Pass the filename.");
         return
     }
     let file_name = &args[1];
     let mut table =rustql::table::table::Table::db_open(file_name);
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









