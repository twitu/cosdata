use super::buffered_io::{BufIoError, BufferManagerFactory};
use super::cache_loader::NodeRegistry;
use super::common::WaCustomError;
use super::lazy_load::{FileIndex, LazyItem, SyncPersist};
use super::types::{BytesToRead, FileOffset, HNSWLevel, MergedNode, NodeProp, VectorId};
use crate::models::serializer::CustomSerialize;
use arcshift::ArcShift;
use std::fs::File;
use std::io::{SeekFrom, Write};
use std::sync::Arc;

pub fn read_node_from_file(
    file_index: FileIndex,
    cache: Arc<NodeRegistry>,
) -> Result<MergedNode, BufIoError> {
    // Deserialize the MergedNode using the FileIndex
    let node: MergedNode = cache.load_item(file_index.clone())?;

    // Pretty print the node
    match file_index {
        FileIndex::Valid { offset, version } => {
            println!(
                "Read MergedNode from offset: {}, version: {}",
                offset.0, *version
            );
        }
        FileIndex::Invalid => {
            println!("Attempted to read MergedNode with an invalid FileIndex");
        }
    }

    // You might want to add more detailed printing here, depending on what information
    // you want to see about the node
    // println!("{:#?}", node);

    Ok(node)
}
pub fn write_node_to_file(
    lazy_item: &LazyItem<MergedNode>,
    bufmans: Arc<BufferManagerFactory>,
    file_index: Option<FileIndex>,
) -> Result<FileIndex, WaCustomError> {
    let mut node_arc = lazy_item
        .get_data()
        .ok_or(WaCustomError::LazyLoadingError("node in null".to_string()))?;
    let node = node_arc.get();
    let version = match file_index {
        Some(FileIndex::Valid { version, .. }) => version,
        _ => lazy_item.get_current_version(),
    };
    let bufman = bufmans.get(&version)?;
    let cursor = bufman.open_cursor()?;

    match file_index {
        Some(FileIndex::Valid {
            offset: FileOffset(offset),
            version,
        }) => {
            println!(
                "About to write at offset {}, version {}, node: {:#?}",
                offset, *version, node
            );
            bufman.seek_with_cursor(cursor, SeekFrom::Start(offset as u64))?;
        }
        Some(FileIndex::Invalid) | None => {
            println!("About to write node at the end of file: {:#?}", node);
            bufman.seek_with_cursor(cursor, SeekFrom::End(0))?;
        }
    }

    let new_offset = node.serialize(bufmans, version, cursor)?;

    bufman.close_cursor(cursor)?;

    // Create and return the new FileIndex
    let new_file_index = FileIndex::Valid {
        offset: FileOffset(new_offset),
        version: match file_index {
            Some(FileIndex::Valid { version, .. }) => version,
            _ => lazy_item.get_current_version(),
        },
    };

    Ok(new_file_index)
}

pub fn persist_node_update_loc(
    bufmans: Arc<BufferManagerFactory>,
    node: &mut ArcShift<LazyItem<MergedNode>>,
) -> Result<(), WaCustomError> {
    let lazy_item = node.get();
    let current_file_index = lazy_item.get_file_index();

    // Write the node to file
    let new_file_index = write_node_to_file(node.get(), bufmans, current_file_index)?;

    // Update the file index in the lazy item
    node.rcu(|lazy_item| {
        let updated_item = lazy_item.clone();
        updated_item.set_file_index(Some(new_file_index));
        updated_item
    });

    Ok(())
}
//
pub fn load_vector_id_lsmdb(_level: HNSWLevel, _vector_id: VectorId) -> LazyItem<MergedNode> {
    LazyItem::Invalid
}

pub fn load_neighbor_persist_ref(_level: HNSWLevel, _node_file_ref: u32) -> Option<MergedNode> {
    None
}
pub fn write_prop_to_file(prop: &NodeProp, mut file: &File) -> (FileOffset, BytesToRead) {
    let mut prop_bytes = Vec::new();
    //let result = encode(&prop);
    let result = serde_cbor::to_vec(&prop).unwrap();

    prop_bytes.extend_from_slice(result.as_ref());

    file.write_all(&prop_bytes)
        .expect("Failed to write to file");
    let offset = file.metadata().unwrap().len() - prop_bytes.len() as u64;
    (
        FileOffset(offset as u32),
        BytesToRead(prop_bytes.len() as u32),
    )
}
