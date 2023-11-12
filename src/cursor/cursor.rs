 use crate::node::{get_leaf_node_num_cells, get_leaf_node_value_ptr};
 use crate::node::node::{find_key_in_leaf_node, get_node_type, NodeType};
 use crate::table::row::ROW_SIZE;
use crate::table::table::{Table, ROWS_PER_PAGE};



pub struct Cursor {
    pub page_num:u32,
    pub cell_num:u32,
    pub end_of_table: bool,
}

impl Cursor {
    pub unsafe fn table_start(table: &mut Table) -> Cursor {
        let root_node_result = table.pager.get_page(table.root_page_num);
        let num_cells = get_leaf_node_num_cells(root_node_result.unwrap());
        Cursor {
            page_num: table.root_page_num,
            cell_num:0,
            end_of_table: (num_cells == 0),
        }
    }

    pub unsafe fn table_end(table: &mut Table) -> Cursor {
        let num_rows = table.num_rows;
        let root_node_result = table.pager.get_page(table.root_page_num);
        let num_cells = get_leaf_node_num_cells(root_node_result.unwrap());

        Cursor {
            page_num:table.root_page_num,
            cell_num: num_cells,
            end_of_table: true,
        }
    }
    //Return the position of key. If key is not found
     //return the position where it should be inserted.
    pub unsafe fn find_key(table: &mut Table,key:u32)->Cursor{
        let root_node = table.pager.get_page(table.root_page_num).unwrap();
        let node_type = get_node_type (root_node) ;
        if(node_type == NodeType::LEAF){
           return find_key_in_leaf_node(table,table.root_page_num,key)
        }
        else {
            panic!("Not implemented searching in internal node")
        }

    }

    pub unsafe fn cursor_value(&mut self, table: &mut Table) -> *mut u8 {
        let cell_num = self.cell_num;
        let page_num = self.page_num;
        let  page_ptr = table.pager.get_page(page_num).unwrap();
        let value_ptr = get_leaf_node_value_ptr(page_ptr,cell_num);
        value_ptr

    }

    pub unsafe fn advance_cursor(&mut self, table: &mut Table) {
        let page_num = self.page_num;
        let node_ptr = table.pager.get_page(page_num).unwrap();
        let num_cells = get_leaf_node_num_cells(node_ptr);
        self.cell_num += 1;
        if (self.cell_num >= num_cells) {
            self.end_of_table = true
        }
    }
}
