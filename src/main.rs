use std::io;
use std::io::Write;
use std::process::exit;

fn main() {
   while(true){
      print_prompt();
      let mut line = String::new();
      let command_string = std::io::stdin().read_line(&mut line).unwrap();
      let command = line.trim();
      if(command == ".exit"){
         exit(0)
      }
      else {
         println!("Unrecognised command '{}'", command)
      }

   }
    
}

fn print_prompt(){
   print!("db > ");
   io::stdout().flush();

}


