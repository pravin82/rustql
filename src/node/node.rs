use crate::cursor::cursor::Cursor;
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
#[derive(Debug, PartialEq)]
pub enum NodeType {
    INTERNAL = 1,
    LEAF = 2,
}

impl NodeType {
    fn from_value(value: u8) -> Option<NodeType> {
        match value {
            1 => Some(NodeType::LEAF),
            2 => Some(NodeType::INTERNAL),
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

unsafe fn leaf_node_num_cells_ptr(node_ptr: *const u8) -> *const u8 {
    node_ptr.add(LEAF_NODE_NUM_CELLS_OFFSET as usize)
}

pub unsafe fn get_leaf_node_num_cells(node_ptr: *const u8) -> u32 {
    let num_cell_ptr = leaf_node_num_cells_ptr(node_ptr);
    let mut bytes = [0; 4];
    for i in 0..4 {
        bytes[i] = ptr::read(num_cell_ptr.add(i))
    }
    u32::from_be_bytes(bytes)
}

unsafe fn get_leaf_node_cell_ptr(node_ptr: *const u8, cell_num: u32) -> *mut u8 {
    node_ptr
        .add(LEAF_NODE_HEADER_SIZE as usize)
        .add((cell_num * LEAF_NODE_CELL_SIZE) as usize)
        .cast_mut()
}

pub unsafe fn get_leaf_node_value_ptr(node_ptr: *const u8, cell_num: u32) -> *mut u8 {
    get_leaf_node_cell_ptr(node_ptr, cell_num).add(LEAF_NODE_KEY_SIZE as usize)
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

pub unsafe fn initialize_leaf_node(node_ptr: *mut u8) {
    set_node_type(node_ptr, NodeType::LEAF);
    set_leaf_node_num_cells(node_ptr, 0)
}

unsafe fn set_leaf_node_num_cells(node_ptr: *const u8, num_cells: u32) {
    let leaf_node_cell_ptr = leaf_node_num_cells_ptr(node_ptr).cast_mut();
    let x = u32::to_be_bytes(num_cells);
    for i in 0..4 {
        let id_byte = x[i];
        ptr::copy(&id_byte, leaf_node_cell_ptr.add(i), 1);
    }
}

pub unsafe fn leaf_node_insert(cursor: Cursor, key: u32, value: Row) -> Result<String, Error> {
    let node_ptr = cursor.table.pager.get_page(cursor.page_num).unwrap();
    let num_cells = get_leaf_node_num_cells(node_ptr);
    if (num_cells >= LEAF_NODE_MAX_CELLS) {
        panic!("Leaf node full. Implement splitting of nodes")
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
    let value_ptr = get_leaf_node_value_ptr(node_ptr, cursor.cell_num);
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
    let num_cells = get_leaf_node_num_cells(node_ptr);
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

pub unsafe fn print_leaf_node(node_ptr: *const u8, mut writer: impl Write) {
    let num_cells = get_leaf_node_num_cells(node_ptr);
    writeln!(writer, "leaf (size {})", num_cells);
    for i in (0..num_cells) {
        let key = get_leaf_node_key(node_ptr, i);
        writeln!(writer, "  - {} : {}", i, key);
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
    ptr::write(node_type_ptr, node_type.into())
}

pub unsafe fn split_leaf_node_and_insert(cursor: Cursor, mut table: Table, key: u32, value: Row) {
    let old_node_ptr = table.pager.get_page(cursor.page_num);
}
