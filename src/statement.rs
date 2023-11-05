
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
    pub(crate) fn prepare_statement(command:&str) -> Statement{
        let chunks: Vec<&str> =  command.split(" ").collect();
        let statement_type = StatementType::from_str(chunks[0]).expect("Wrong statement");
        let mut row_to_insert = Row {
            id:0,
            username:['a';COLUMN_USERNAME_SIZE],
            email:['a';COLUMN_EMAIL_SIZE],
        };
        if statement_type == StatementType::INSERT {
            let id_str = chunks[1];
            let id = id_str.parse().unwrap();
            let mut username: [char; COLUMN_USERNAME_SIZE] = ['\0'; COLUMN_USERNAME_SIZE];
            let mut email: [char;COLUMN_EMAIL_SIZE] = ['\0'; COLUMN_EMAIL_SIZE];
            for (i, c) in chunks[2].chars().take(COLUMN_USERNAME_SIZE).enumerate() {
                username[i] = c;
            }
            for (i, c) in chunks[3].chars().take(COLUMN_USERNAME_SIZE).enumerate() {
                email[i] = c;
            }
            row_to_insert = Row{id,username,email};
            


        }

        Statement{
            statement_type,
            row_to_insert: row_to_insert,
        }

    }
}