use std::io::{Error, ErrorKind};
use std::str::FromStr;
use crate::table::row::{COLUMN_EMAIL_SIZE, COLUMN_USERNAME_SIZE, Row};

#[derive(PartialEq)]
pub enum StatementType{
    INSERT,
    UPDATE,
    SELECT
}

impl FromStr for StatementType {

    type Err = ();

    fn from_str(input: &str) -> Result<StatementType, Self::Err> {
        match input {
            "insert"  => Ok(StatementType::INSERT),
            "update"  => Ok(StatementType::UPDATE),
            "select"  => Ok(StatementType::SELECT),
            _      => Err(()),
        }
    }
}

pub struct Statement{
    pub(crate) statement_type :StatementType,
   pub row_to_insert: Row
}

impl Statement{
    pub(crate) fn prepare_statement(command:&str) -> Result<Statement,Error>{
        let chunks: Vec<&str> =  command.split(' ').collect();
        let statement_type = StatementType::from_str(chunks[0]).expect("Wrong statement");
        let mut row_to_insert = Row {
            id:0,
            username:['a';COLUMN_USERNAME_SIZE],
            email:['a';COLUMN_EMAIL_SIZE],
        };
        if statement_type == StatementType::INSERT {
            let id_str = chunks[1];
            let username_chunks = chunks[2];
            let email_chunks = chunks[3];
            if(username_chunks.len()> COLUMN_USERNAME_SIZE || email_chunks.len() > COLUMN_EMAIL_SIZE){
                  return Err(Error::new(ErrorKind::Other,"String is too long."));
            }
            let parsed_id:i32 = id_str.parse().unwrap();
            if(parsed_id < 0){ return Err(Error::new(ErrorKind::Other,"Id must be positive."));}
            let id = parsed_id as u32;
            let mut username: [char; COLUMN_USERNAME_SIZE] = ['\0'; COLUMN_USERNAME_SIZE];
            let mut email: [char;COLUMN_EMAIL_SIZE] = ['\0'; COLUMN_EMAIL_SIZE];
            for (i, c) in username_chunks.chars().take(COLUMN_USERNAME_SIZE).enumerate() {
                username[i] = c;
            }
            for (i, c) in email_chunks.chars().take(COLUMN_EMAIL_SIZE).enumerate() {
                email[i] = c;
            }
            row_to_insert = Row{id,username,email};
            


        }

       Ok( Statement{
            statement_type,
            row_to_insert,
        }
       )

    }
}