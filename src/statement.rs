use std::str::FromStr;
use crate::table::row::Row;


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

struct Statement{
    statement_type :StatementType,
    row_to_insert: Row
}