use std::str::FromStr;

pub enum Statement{
    INSERT,
    UPDATE,
    SELECT
}

impl FromStr for Statement {

    type Err = ();

    fn from_str(input: &str) -> Result<Statement, Self::Err> {
        match input {
            "insert"  => Ok(Statement::INSERT),
            "update"  => Ok(Statement::UPDATE),
            "select"  => Ok(Statement::SELECT),
            _      => Err(()),
        }
    }
}