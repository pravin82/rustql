use crate::cursor::cursor::Cursor;
use crate::pager::pager::Pager;
use crate::table::row::{Row, ROW_SIZE};
use crate::table::table::{Table, PAGE_SIZE};
use std::io::{Error, ErrorKind, Write};
use std::mem::size_of;
use std::ptr;

//Common header
const NODE_TYPE_SIZE: u32 = size_of::<u8>() as u32;
const NODE_TYPE_OFFSET: u32 = 0;
const IS_ROOT_SIZE: u32 = size_of::<u8>() as u32;
const IS_ROOT_OFFSET: u32 = NODE_TYPE_SIZE as u32;
const PARENT_POINTER_SIZE: u32 = size_of::<u32>() as u32;
const PARENT_POINTER_OFFSET: u32 = IS_ROOT_OFFSET + IS_ROOT_SIZE as u32;
const COMMON_NODE_HEADER_SIZE: u32 = (NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE) as u32;

//Leaf node header

const LEAF_NODE_NUM_CELLS_SIZE: u32 = size_of::<u32>() as u32;
const LEAF_NODE_NUM_CELLS_OFFSET: u32 = COMMON_NODE_HEADER_SIZE as u32;
const LEAF_NODE_HEADER_SIZE: u32 = COMMON_NODE_HEADER_SIZE as u32 + LEAF_NODE_NUM_CELLS_SIZE;

//Leaf node body

const LEAF_NODE_KEY_SIZE: u32 = size_of::<u32>() as u32;
const LEAF_NODE_KEY_OFFSET: u32 = 0; //This is relative to body
const LEAF_NODE_VALUE_SIZE: u32 = ROW_SIZE as u32;
const LEAF_NODE_VALUE_OFFSET: u32 = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const LEAF_NODE_CELL_SIZE: u32 = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const LEAF_NODE_SPACE_FOR_CELLS: u32 = (PAGE_SIZE - LEAF_NODE_HEADER_SIZE as usize) as u32;
const LEAF_NODE_MAX_CELLS: u32 = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;
const LEAF_NODE_RIGHT_SPLIT_CELL_COUNT: u32 = (LEAF_NODE_MAX_CELLS + 1) / 2;
const LEAF_NODE_LEFT_SPLIT_CELL_COUNT: u32 =
    (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_CELL_COUNT;

const INTERNAL_NODE_NUM_KEYS_SIZE: u32 = size_of::<u32>() as u32;
const INTERNAL_NODE_NUM_KEYS_OFFSET: u32 = COMMON_NODE_HEADER_SIZE;
const INTERNAL_NODE_RIGHT_CHILD_SIZE: u32 = size_of::<u32>() as u32;
const INTERNAL_NODE_RIGHT_CHILD_OFFSET: u32 = COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE;
const INTERNAL_NODE_HEADER_SIZE: u32 =
    COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE;
const INTERNAL_NODE_KEY_SIZE: u32 = size_of::<u32>() as u32;
const INTERNAL_NODE_CHILD_SIZE: u32 = size_of::<u32>() as u32;
const INTERNAL_NODE_CELL_SIZE: u32 = INTERNAL_NODE_KEY_SIZE + INTERNAL_NODE_CHILD_SIZE;
#[derive(Debug, PartialEq)]
pub enum NodeType {
    INTERNAL = 1,
    LEAF = 2,
}

impl NodeType {
    fn from_value(value: u8) -> Option<NodeType> {
        match value {
            1 => Some(NodeType::INTERNAL),
            2 => Some(NodeType::LEAF),
            _ => None,
        }
    }
}

impl From<NodeType> for u8 {
    fn from(node_type: NodeType) -> Self {
        match node_type {
            NodeType::LEAF => 1,
            NodeType::INTERNAL => 2,
        }
    }
}

pub struct Node;

impl Node {
    pub unsafe fn get_leaf_node_num_cells(node_ptr: *const u8) -> u32 {
        let num_cell_ptr = leaf_node_num_cells_ptr(node_ptr);
        let mut bytes = [0; 4];
        for i in 0..4 {
            bytes[i] = ptr::read(num_cell_ptr.add(i))
        }
        u32::from_be_bytes(bytes)
    }

    pub unsafe fn get_leaf_node_value_ptr(node_ptr: *const u8, cell_num: u32) -> *mut u8 {
        get_leaf_node_cell_ptr(node_ptr, cell_num).add(LEAF_NODE_KEY_SIZE as usize)
    }

    pub unsafe fn initialize_leaf_node(node_ptr: *mut u8) {
        Node::set_node_type(node_ptr, NodeType::LEAF);
        Node::set_node_root(node_ptr, false);
        set_leaf_node_num_cells(node_ptr, 0)
    }

    pub unsafe fn initialize_internal_node(node_ptr: *mut u8) {
        Node::set_node_type(node_ptr, NodeType::INTERNAL);
        Node::set_node_root(node_ptr, false);
        set_internal_node_num_keys(node_ptr, 0)
    }

    pub unsafe fn leaf_node_insert(cursor: Cursor, key: u32, value: Row) -> Result<String, Error> {
        let node_ptr = cursor.table.pager.get_page(cursor.page_num).unwrap();
        let num_cells = Node::get_leaf_node_num_cells(node_ptr);
        if (num_cells >= LEAF_NODE_MAX_CELLS) {
            Node::split_leaf_node_and_insert(cursor, key, value);
            return Ok("EXECUTE_SUCCESS".parse().unwrap());
        }
        let key_at_index = get_leaf_node_key(node_ptr, cursor.cell_num);
        if (key_at_index == key) {
            return (Err(Error::new(ErrorKind::Other, "Error:Duplicate key")));
        }
        if (cursor.cell_num < num_cells) {
            for i in (cursor.cell_num + 1..=num_cells).rev() {
                let src_cell_ptr = get_leaf_node_cell_ptr(node_ptr, i - 1);
                let dest_ptr = get_leaf_node_cell_ptr(node_ptr, i);
                ptr::copy(src_cell_ptr, dest_ptr, LEAF_NODE_CELL_SIZE as usize);
            }
        }

        let cell_ptr = get_leaf_node_cell_ptr(node_ptr, cursor.cell_num);
        let value_ptr = Node::get_leaf_node_value_ptr(node_ptr, cursor.cell_num);
        set_leaf_node_num_cells(node_ptr, num_cells + 1);
        let x = u32::to_be_bytes(key);
        for i in 0..4 {
            let id_byte = x[i];
            ptr::copy(&id_byte, cell_ptr.add(i), 1);
        }
        value.serialize_row(value_ptr);
        Ok("EXECUTE_SUCCESS".parse().unwrap())
    }

    pub unsafe fn find_key_in_leaf_node<'a>(
        table: &'a mut Table,
        page_num: u32,
        key: u32,
    ) -> Cursor<'a> {
        let node_ptr = table.pager.get_page(page_num).unwrap();
        let num_cells = Node::get_leaf_node_num_cells(node_ptr);
        let mut required_index = 0;
        let mut min_index = 0;
        let mut one_past_max_index = num_cells;
        while (one_past_max_index != min_index) {
            let index = (one_past_max_index + min_index) / 2;
            let key_at_index = get_leaf_node_key(node_ptr, index);
            if (key > key_at_index) {
                min_index = index + 1
            } else if (key < key_at_index) {
                one_past_max_index = index
            } else {
                required_index = index;
                break;
            }
        }
        required_index = min_index;
        let cursor = Cursor {
            page_num,
            cell_num: required_index,
            end_of_table: false,
            table,
        };
        return cursor;
    }

    pub(crate) unsafe fn find_key_in_internal_node<'a>(
        table: &'a mut Table,
        page_num: u32,
        key: u32,
    ) -> Cursor<'a> {
        let node_ptr = table.pager.get_page(page_num).unwrap();
        let num_keys = get_internal_node_num_keys(node_ptr);
        let mut min_index = 0;
        let mut max_index = num_keys;
        while (min_index != max_index) {
            let index = (max_index + min_index) / 2;
            let key_at_index = get_internal_node_key(node_ptr, index);
            if (key > key_at_index) {
                min_index = index + 1
            } else {
                max_index = index
            }
        }
        let child_node_page_num = get_internal_node_child_page_num(node_ptr, min_index);
        let child_ptr = table.pager.get_page(child_node_page_num).unwrap();
        let child_type = Node::get_node_type(child_ptr);
        let cursor = match child_type {
            NodeType::INTERNAL => Node::find_key_in_internal_node(table, child_node_page_num, key),
            NodeType::LEAF => Node::find_key_in_leaf_node(table, child_node_page_num, key),
        };

        cursor
    }

    pub unsafe fn print_leaf_node(node_ptr: *const u8, mut writer: impl Write) {
        let num_cells = Node::get_leaf_node_num_cells(node_ptr);
        writeln!(writer, "leaf (size {})", num_cells);
        for i in (0..num_cells) {
            let key = get_leaf_node_key(node_ptr, i);
            writeln!(writer, "  - {} : {}", i, key);
        }
    }
    pub unsafe fn print_tree(
        pager: &mut Pager,
        page_num: u32,
        indentation_level: u32,
        writer: &mut impl Write,
    ) {
        let node_ptr = pager.get_page(page_num).unwrap();
        let node_type = Node::get_node_type(node_ptr);
        match node_type {
            NodeType::LEAF => {
                let num_keys = get_leaf_node_num_cells(node_ptr);
                Node::indent(indentation_level, writer);
                writeln!(writer, "- leaf (size {})", num_keys);
                for i in 0..num_keys {
                    Node::indent(indentation_level + 1, writer);
                    writeln!(writer, "- {}", get_leaf_node_key(node_ptr, i));
                }
            }
            NodeType::INTERNAL => {
                let num_keys = get_internal_node_num_keys(node_ptr);
                Node::indent(indentation_level, writer);
                writeln!(writer, "- internal (size {})", num_keys);
                for i in 0..num_keys {
                    let child_num = get_internal_node_child_page_num(node_ptr, i);
                    Node::print_tree(pager, child_num, indentation_level + 1, writer);
                    Node::indent(indentation_level + 1, writer);
                    writeln!(writer, "- key {}", get_internal_node_key(node_ptr, i));
                }
                let child_num = get_internal_node_right_child(node_ptr);
                Node::print_tree(pager, child_num, indentation_level + 1, writer);
            }
        }
    }

    fn indent(level: u32, writer: &mut impl Write) {
        for i in 0..level {
            write!(writer, "  ");
        }
    }

    pub unsafe fn get_node_type(node_ptr: *const u8) -> NodeType {
        let node_type_ptr = node_ptr.add(NODE_TYPE_OFFSET as usize);
        let node_type_value = std::ptr::read(node_type_ptr);
        let node_type = NodeType::from_value(node_type_value).unwrap();
        return node_type;
    }

    pub unsafe fn set_node_type(node_ptr: *mut u8, node_type: NodeType) {
        let node_type_ptr = node_ptr.add(NODE_TYPE_OFFSET as usize);
        ptr::write(node_type_ptr, node_type as u8)
    }

    pub unsafe fn set_node_root(node_ptr: *mut u8, is_root: bool) {
        let is_root_ptr = node_ptr.add(IS_ROOT_OFFSET as usize);
        ptr::copy(&(is_root as u8), is_root_ptr, 1);
    }

    pub unsafe fn split_leaf_node_and_insert(cursor: Cursor, key: u32, value: Row) {
        let old_node_ptr = cursor.table.pager.get_page(cursor.page_num).unwrap();
        let new_page_num = cursor.table.pager.get_unused_page_num();
        let new_node_ptr = cursor.table.pager.get_page(new_page_num).unwrap();
        Node::initialize_leaf_node(new_node_ptr);
        //Shifting extra cells from left node to right node
        for i in LEAF_NODE_LEFT_SPLIT_CELL_COUNT..LEAF_NODE_MAX_CELLS {
            let cell_ptr = get_leaf_node_cell_ptr(old_node_ptr, i);
            let destination_cell_ptr =
                get_leaf_node_cell_ptr(new_node_ptr, i - LEAF_NODE_LEFT_SPLIT_CELL_COUNT);
            ptr::copy(cell_ptr, destination_cell_ptr, LEAF_NODE_CELL_SIZE as usize);
            ptr::copy(&0u8, cell_ptr, LEAF_NODE_CELL_SIZE as usize)
        }
        //Inserting new node
        let new_cell_ptr =
            get_leaf_node_cell_ptr(new_node_ptr, LEAF_NODE_RIGHT_SPLIT_CELL_COUNT - 1);
        let value_ptr =
            Node::get_leaf_node_value_ptr(new_node_ptr, LEAF_NODE_RIGHT_SPLIT_CELL_COUNT - 1);
        let x = u32::to_be_bytes(key);
        for i in 0..4 {
            let id_byte = x[i];
            ptr::copy(&id_byte, new_cell_ptr.add(i), 1);
        }
        value.serialize_row(value_ptr);

        //Set the size of nodes
        set_leaf_node_num_cells(old_node_ptr, LEAF_NODE_LEFT_SPLIT_CELL_COUNT);
        set_leaf_node_num_cells(new_node_ptr, LEAF_NODE_RIGHT_SPLIT_CELL_COUNT);
        if (is_node_root(old_node_ptr)) {
            Node::create_new_root(cursor.table, new_page_num)
        } else {
            panic!("Need to implement updating parent after split\n")
        }
    }
    //current root data will be copied to left child
    unsafe fn create_new_root(table: &mut Table, right_child_page_num: u32) {
        let root_page = table.pager.get_page(table.root_page_num).unwrap();
        let left_child_page_num = table.pager.get_unused_page_num();
        let left_child_page = table.pager.get_page(left_child_page_num).unwrap();
        ptr::copy(root_page, left_child_page, PAGE_SIZE);
        Node::set_node_root(left_child_page, false);
        Node::initialize_internal_node(root_page);
        Node::set_node_root(root_page, true);
        set_internal_node_num_keys(root_page, 1);
        let left_child_max_key = get_node_max_key(left_child_page);
        set_internal_node_cell(root_page, 0, left_child_page_num, left_child_max_key);
        set_internal_node_right_child(root_page, right_child_page_num)
    }
}

unsafe fn leaf_node_num_cells_ptr(node_ptr: *const u8) -> *const u8 {
    node_ptr.add(LEAF_NODE_NUM_CELLS_OFFSET as usize)
}

unsafe fn get_leaf_node_cell_ptr(node_ptr: *const u8, cell_num: u32) -> *mut u8 {
    node_ptr
        .add(LEAF_NODE_HEADER_SIZE as usize)
        .add((cell_num * LEAF_NODE_CELL_SIZE) as usize)
        .cast_mut()
}

unsafe fn set_leaf_node_num_cells(node_ptr: *const u8, num_cells: u32) {
    let leaf_node_cell_ptr = leaf_node_num_cells_ptr(node_ptr).cast_mut();
    let x = u32::to_be_bytes(num_cells);
    for i in 0..4 {
        let id_byte = x[i];
        ptr::copy(&id_byte, leaf_node_cell_ptr.add(i), 1);
    }
}

unsafe fn get_leaf_node_num_cells(node_ptr: *const u8) -> u32 {
    let leaf_node_cell_ptr = leaf_node_num_cells_ptr(node_ptr).cast_mut();
    let mut bytes = [0; 4];
    for i in 0..4 {
        bytes[i] = ptr::read(leaf_node_cell_ptr.add(i))
    }
    u32::from_be_bytes(bytes)
}

unsafe fn set_leaf_node_key(cell_ptr: *mut u8, key: u32) {
    let bytes = u32::to_be_bytes(key);
    for i in 0..4 {
        let id_byte = bytes[i];
        ptr::copy(&id_byte, cell_ptr.add(i), 1);
    }
}

unsafe fn get_leaf_node_key(node_ptr: *const u8, cell_num: u32) -> u32 {
    let cell_ptr = get_leaf_node_cell_ptr(node_ptr, cell_num);
    let mut bytes = [0; 4];
    for i in 0..4 {
        bytes[i] = ptr::read(cell_ptr.add(i))
    }
    u32::from_be_bytes(bytes)
}

unsafe fn is_node_root(node_ptr: *mut u8) -> bool {
    let is_root_ptr = node_ptr.add(IS_ROOT_OFFSET as usize);
    let is_root_value = std::ptr::read(is_root_ptr);
    let is_root = is_root_value != 0;
    return is_root;
}

unsafe fn set_internal_node_num_keys(node_ptr: *mut u8, num_keys: u32) {
    let num_key_offset = node_ptr.add(INTERNAL_NODE_NUM_KEYS_OFFSET as usize);
    let bytes = u32::to_be_bytes(num_keys);
    for i in 0..4 {
        let id_byte = bytes[i];
        ptr::copy(&id_byte, num_key_offset.add(i), 1);
    }
}

unsafe fn get_internal_node_num_keys(node_ptr: *mut u8) -> u32 {
    let num_key_ptr = node_ptr.add(INTERNAL_NODE_NUM_KEYS_OFFSET as usize);
    let mut bytes = [0; 4];
    for i in 0..4 {
        bytes[i] = ptr::read(num_key_ptr.add(i))
    }
    u32::from_be_bytes(bytes)
}

unsafe fn get_internal_node_cell_ptr(node_ptr: *mut u8, cell_num: u32) -> *mut u8 {
    node_ptr.add((INTERNAL_NODE_HEADER_SIZE + cell_num * INTERNAL_NODE_CELL_SIZE) as usize)
}

unsafe fn get_internal_node_key(node_ptr: *mut u8, cell_num: u32) -> u32 {
    let cell_ptr = get_internal_node_cell_ptr(node_ptr, cell_num);
    let key_ptr = cell_ptr.add(INTERNAL_NODE_CHILD_SIZE as usize);
    let mut bytes = [0; 4];
    for i in 0..4 {
        bytes[i] = ptr::read(key_ptr.add(i))
    }
    u32::from_be_bytes(bytes)
}

unsafe fn get_internal_node_child_page_num(node_ptr: *mut u8, child_num: u32) -> u32 {
    let num_keys = get_internal_node_num_keys(node_ptr);
    if (child_num > num_keys) {
        panic!("Child number passed is greater than num of keys in node");
    } else if (child_num == num_keys) {
        return get_internal_node_right_child(node_ptr);
    } else {
        let cell_ptr = get_internal_node_cell_ptr(node_ptr, child_num);
        let mut bytes = [0; 4];
        for i in 0..4 {
            bytes[i] = ptr::read(cell_ptr.add(i))
        }
        u32::from_be_bytes(bytes)
    }
}

unsafe fn set_internal_node_right_child(node_ptr: *mut u8, page_num: u32) {
    let right_child_ptr = node_ptr.add(INTERNAL_NODE_RIGHT_CHILD_OFFSET as usize);
    let bytes = u32::to_be_bytes(page_num);
    for i in 0..4 {
        let id_byte = bytes[i];
        ptr::copy(&id_byte, right_child_ptr.add(i), 1);
    }
}

unsafe fn get_internal_node_right_child(node_ptr: *mut u8) -> u32 {
    let right_child_ptr = node_ptr.add(INTERNAL_NODE_RIGHT_CHILD_OFFSET as usize);
    let mut bytes = [0; 4];
    for i in 0..4 {
        bytes[i] = ptr::read(right_child_ptr.add(i))
    }
    u32::from_be_bytes(bytes)
}
unsafe fn set_internal_node_cell(node_ptr: *mut u8, cell_num: u32, child_page_num: u32, key: u32) {
    let cell_ptr =
        node_ptr.add((INTERNAL_NODE_HEADER_SIZE + cell_num * INTERNAL_NODE_CELL_SIZE) as usize);
    let bytes = u32::to_be_bytes(child_page_num);
    for i in 0..4 {
        let id_byte = bytes[i];
        ptr::copy(&id_byte, cell_ptr.add(i), 1);
    }
    let cell_key_ptr = cell_ptr.add(INTERNAL_NODE_CHILD_SIZE as usize);
    let bytes = u32::to_be_bytes(key);
    for i in 0..4 {
        let id_byte = bytes[i];
        ptr::copy(&id_byte, cell_key_ptr.add(i), 1);
    }
}

unsafe fn get_node_max_key(node_ptr: *mut u8) -> u32 {
    match Node::get_node_type(node_ptr) {
        NodeType::INTERNAL => {
            let num_keys = get_internal_node_num_keys(node_ptr);
            get_internal_node_key(node_ptr, num_keys - 1)
        }
        NodeType::LEAF => {
            let num_cells = Node::get_leaf_node_num_cells(node_ptr);
            get_leaf_node_key(node_ptr, num_cells - 1)
        }
    }
}
