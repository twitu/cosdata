use super::CustomSerialize;
use crate::models::buffered_io::{BufIoError, BufferManagerFactory};
use crate::models::cache_loader::NodeRegistry;
use crate::models::lazy_load::{FileIndex, LazyItem, LazyItemMap};
use crate::models::types::FileOffset;
use crate::models::versioning::Hash;
use crate::storage::inverted_index::InvertedIndexItem;
use std::collections::HashSet;
use std::io::{self, SeekFrom};
use std::sync::Arc;

impl<T> CustomSerialize for InvertedIndexItem<T>
where
    T: Clone + CustomSerialize + 'static,
    LazyItem<T>: CustomSerialize,
    InvertedIndexItem<T>: Clone,
{
    fn serialize(
        &self,
        bufmans: Arc<BufferManagerFactory>,
        version: Hash,
        cursor: u64,
    ) -> Result<u32, BufIoError> {
        let bufman = bufmans.get(&version)?;
        let start_offset = bufman.cursor_position(cursor)? as u32;

        // Serialize dim_index
        bufman.write_u32_with_cursor(cursor, self.dim_index)?;

        // Serialize implicit flag
        bufman.write_u8_with_cursor(cursor, self.implicit as u8)?;

        // Serialize data
        let data_offset = self.data.serialize(bufmans.clone(), version, cursor)?;
        bufman.write_u32_with_cursor(cursor, data_offset)?;

        // Serialize lazy_children
        let children_offset = self
            .lazy_children
            .serialize(bufmans.clone(), version, cursor)?;
        bufman.write_u32_with_cursor(cursor, children_offset)?;

        Ok(start_offset)
    }

    fn deserialize(
        bufmans: Arc<BufferManagerFactory>,
        file_index: FileIndex,
        cache: Arc<NodeRegistry>,
        max_loads: u16,
        skipm: &mut HashSet<u64>,
    ) -> Result<Self, BufIoError> {
        match file_index {
            FileIndex::Invalid => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot deserialize InvertedIndexItem with an invalid FileIndex",
            )
            .into()),
            FileIndex::Valid {
                offset: FileOffset(offset),
                version,
            } => {
                let bufman = bufmans.get(&version)?;
                let cursor = bufman.open_cursor()?;
                bufman.seek_with_cursor(cursor, SeekFrom::Start(offset as u64))?;

                // Deserialize dim_index
                let dim_index = bufman.read_u32_with_cursor(cursor)?;

                // Deserialize implicit flag
                let implicit = bufman.read_u8_with_cursor(cursor)? != 0;

                // Deserialize data
                let data_offset = bufman.read_u32_with_cursor(cursor)?;
                let data_file_index = FileIndex::Valid {
                    offset: FileOffset(data_offset),
                    version,
                };
                let data = LazyItemMap::deserialize(
                    bufmans.clone(),
                    data_file_index,
                    cache.clone(),
                    max_loads,
                    skipm,
                )?;

                // Deserialize lazy_children
                let children_offset = bufman.read_u32_with_cursor(cursor)?;
                let children_file_index = FileIndex::Valid {
                    offset: FileOffset(children_offset),
                    version,
                };
                let lazy_children = LazyItemMap::deserialize(
                    bufmans.clone(),
                    children_file_index,
                    cache.clone(),
                    max_loads,
                    skipm,
                )?;

                bufman.close_cursor(cursor)?;

                Ok(InvertedIndexItem {
                    dim_index,
                    implicit,
                    data,
                    lazy_children,
                })
            }
        }
    }
}
