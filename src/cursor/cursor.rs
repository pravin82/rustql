use crate::node::node::{Node, NodeType};
use crate::table::row::ROW_SIZE;
use crate::table::table::{Table, ROWS_PER_PAGE};

pub struct Cursor<'a> {
    pub page_num: u32,
    pub cell_num: u32,
    pub end_of_table: bool,
    pub table: &'a mut Table,
}

impl<'a> Cursor<'a> {
    pub unsafe fn table_start(table: &mut Table) -> Cursor {
        let root_node_result = table.pager.get_page(table.root_page_num);
        let num_cells = Node::get_leaf_node_num_cells(root_node_result.unwrap());
        Cursor {
            page_num: table.root_page_num,
            cell_num: 0,
            end_of_table: (num_cells == 0),
            table,
        }
    }

    //Return the position of key. If key is not found
    //return the position where it should be inserted.
    pub unsafe fn find_key(table: &'a mut Table, key: u32) -> Cursor<'a> {
        let root_page_num = table.root_page_num;
        let root_node = table.pager.get_page(root_page_num).unwrap();
        let node_type = Node::get_node_type(root_node);
        return match node_type {
            NodeType::LEAF => Node::find_key_in_leaf_node(table, root_page_num, key),
            NodeType::INTERNAL => Node::find_key_in_internal_node(table, root_page_num, key),
        };
    }

    pub unsafe fn cursor_value(&mut self) -> *mut u8 {
        let cell_num = self.cell_num;
        let page_num = self.page_num;
        let page_ptr = self.table.pager.get_page(page_num).unwrap();
        let value_ptr = Node::get_leaf_node_value_ptr(page_ptr, cell_num);
        value_ptr
    }

    pub unsafe fn advance_cursor(&mut self) {
        let page_num = self.page_num;
        let node_ptr = self.table.pager.get_page(page_num).unwrap();
        let num_cells = Node::get_leaf_node_num_cells(node_ptr);
        self.cell_num += 1;
        if (self.cell_num >= num_cells) {
            self.end_of_table = true
        }
    }
}
