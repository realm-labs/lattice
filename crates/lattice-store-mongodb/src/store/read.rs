//! Typed MongoDB read operations.

use mongodb::bson::{Document, doc};

use crate::document::{
    LoadedScannedDocument, MongoDocument, decode_flat_document, decode_flat_scanned_document,
};
use crate::error::MongoStoreError;
use crate::scan::MongoScan;

use super::{MongoStore, mongo_timeout, store_error};

impl MongoStore {
    #[doc(hidden)]
    pub async fn find_one_scanned<D>(
        &self,
        id: D::Id,
    ) -> Result<Option<LoadedScannedDocument<D>>, MongoStoreError>
    where
        D: MongoScan,
    {
        let collection = self.database.collection::<Document>(D::COLLECTION);
        let id = mongodb::bson::to_bson(&id).map_err(store_error("encode document id"))?;
        let document = mongo_timeout(
            self.operation_timeout,
            "find scanned document",
            collection.find_one(doc! { "_id": id }),
        )
        .await?;
        document.map(decode_flat_scanned_document::<D>).transpose()
    }

    pub async fn find_one<D>(
        &self,
        id: D::Id,
    ) -> Result<Option<crate::document::LoadedDocument<D>>, MongoStoreError>
    where
        D: MongoDocument,
    {
        let collection = self.database.collection::<Document>(D::COLLECTION);
        let id = mongodb::bson::to_bson(&id).map_err(store_error("encode document id"))?;
        let document = mongo_timeout(
            self.operation_timeout,
            "find typed document",
            collection.find_one(doc! { "_id": id }),
        )
        .await?;
        document.map(decode_flat_document::<D>).transpose()
    }

    pub async fn find_many<D>(
        &self,
        filter: Document,
    ) -> Result<Vec<crate::document::LoadedDocument<D>>, MongoStoreError>
    where
        D: MongoDocument,
    {
        let collection = self.database.collection::<Document>(D::COLLECTION);
        let mut cursor = mongo_timeout(
            self.operation_timeout,
            "find typed documents",
            collection.find(filter),
        )
        .await?;
        let mut documents = Vec::new();
        while mongo_timeout(
            self.operation_timeout,
            "advance typed document cursor",
            cursor.advance(),
        )
        .await?
        {
            let document = cursor
                .deserialize_current()
                .map_err(store_error("decode typed document"))?;
            documents.push(decode_flat_document::<D>(document)?);
        }
        Ok(documents)
    }

    #[doc(hidden)]
    pub async fn find_many_scanned<D>(
        &self,
        filter: Document,
    ) -> Result<Vec<LoadedScannedDocument<D>>, MongoStoreError>
    where
        D: MongoScan,
    {
        let collection = self.database.collection::<Document>(D::COLLECTION);
        let mut cursor = mongo_timeout(
            self.operation_timeout,
            "find scanned documents",
            collection.find(filter),
        )
        .await?;
        let mut documents = Vec::new();
        while mongo_timeout(
            self.operation_timeout,
            "advance scanned document cursor",
            cursor.advance(),
        )
        .await?
        {
            let document = cursor
                .deserialize_current()
                .map_err(store_error("decode scanned document"))?;
            documents.push(decode_flat_scanned_document::<D>(document)?);
        }
        Ok(documents)
    }

    pub async fn find_page<D>(
        &self,
        filter: Document,
        sort: Document,
        limit: u32,
    ) -> Result<Vec<crate::document::LoadedDocument<D>>, MongoStoreError>
    where
        D: MongoDocument,
    {
        if limit == 0 {
            return Err(MongoStoreError::invalid_config(
                "page limit",
                "must be positive",
            ));
        }
        let collection = self.database.collection::<Document>(D::COLLECTION);
        let mut cursor = mongo_timeout(
            self.operation_timeout,
            "find typed document page",
            collection.find(filter).sort(sort).limit(i64::from(limit)),
        )
        .await?;
        let mut documents = Vec::with_capacity(limit as usize);
        while mongo_timeout(
            self.operation_timeout,
            "advance typed document page cursor",
            cursor.advance(),
        )
        .await?
        {
            let document = cursor
                .deserialize_current()
                .map_err(store_error("decode typed document page"))?;
            documents.push(decode_flat_document::<D>(document)?);
        }
        Ok(documents)
    }

    #[doc(hidden)]
    pub async fn find_page_scanned<D>(
        &self,
        filter: Document,
        sort: Document,
        limit: u32,
    ) -> Result<Vec<LoadedScannedDocument<D>>, MongoStoreError>
    where
        D: MongoScan,
    {
        if limit == 0 {
            return Err(MongoStoreError::invalid_config(
                "page limit",
                "must be positive",
            ));
        }
        let collection = self.database.collection::<Document>(D::COLLECTION);
        let mut cursor = mongo_timeout(
            self.operation_timeout,
            "find scanned document page",
            collection.find(filter).sort(sort).limit(i64::from(limit)),
        )
        .await?;
        let mut documents = Vec::with_capacity(limit as usize);
        while mongo_timeout(
            self.operation_timeout,
            "advance scanned document page cursor",
            cursor.advance(),
        )
        .await?
        {
            let document = cursor
                .deserialize_current()
                .map_err(store_error("decode scanned document page"))?;
            documents.push(decode_flat_scanned_document::<D>(document)?);
        }
        Ok(documents)
    }
}
