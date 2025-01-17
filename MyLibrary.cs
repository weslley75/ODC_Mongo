﻿using MongoDB.Bson;
using MongoDB.Driver;
using OutSystems.ExternalLibraries.SDK;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;
using MongoDB.Driver.Core.Configuration;

namespace MongoDB_ODC
{
    public class MyLibrary : IMongoDB
    {
        private MongoDBHandler _handler;
        private string _connectionString;
        private string _databaseName;

        public MyLibrary()
        {
        
        }

        public void Initialize(string connectionString, string databaseName)
        {
            _connectionString = connectionString;
            _databaseName = databaseName;
            _handler = new MongoDBHandler(connectionString, databaseName);
        }

        [OSAction]
        public bool ValidateConnection(string connectionString, string databaseName)
        {
            var mongoService = new MongoService(connectionString, databaseName);
            return mongoService.ValidateConnection();
        }

        [OSAction]
        public string GetCollectionDocuments(string collectionName, string connectionString, string databaseName, int skip, int limit)
        {
            var mongoService = new MongoService(connectionString, databaseName);

            if (!mongoService.CollectionExists(collectionName))
            {
                throw new ApplicationException($"A coleção '{collectionName}' não existe no banco de dados '{databaseName}'.");
            }

            var documentCount = mongoService.GetDocumentCount(collectionName);
            if (documentCount == 0)
            {
                throw new ApplicationException($"A coleção '{collectionName}' está vazia.");
            }

            var collection = mongoService.GetCollection(collectionName);
            var bsonList = collection.Find(new BsonDocument()).Skip(skip).Limit(limit).ToList();

            var jsonArray = new BsonArray(bsonList);
            var jsonWriterSettings = new JsonWriterSettings { OutputMode = JsonOutputMode.Shell};
            var json = jsonArray.ToJson(jsonWriterSettings);

            return json;
        }

        [OSAction(Description = "Performs an aggregation operation on a collection and returns the results in JSON format.")]
        public string AggregateCollection(string collectionName, string connectionString, string databaseName, string aggregatePipeline)
        {
            var mongoService = new MongoService(connectionString, databaseName);
            Console.WriteLine($"Verificando se a coleção existe: {collectionName}");

            if (!mongoService.CollectionExists(collectionName))
            {
                throw new ApplicationException($"A coleção '{collectionName}' não existe no banco de dados '{databaseName}'.");
            }

            // Parse the aggregation pipeline JSON into a BsonDocument array
            var bsonPipeline = BsonSerializer.Deserialize<BsonDocument[]>(aggregatePipeline);

            // Execute the aggregation pipeline
            var aggregateResult = mongoService.AggregateCollection(collectionName, bsonPipeline);

            // Convert the result to JSON
            var jsonWriterSettings = new JsonWriterSettings { OutputMode = JsonOutputMode.Shell };
            var jsonResult = aggregateResult.ToJson(jsonWriterSettings);

            return jsonResult;
        }

        [OSAction(Description = "Creates a document in the specified collection.")]
        public void CreateDocument(string connectionString, string databaseName, string collectionName, string documentJson)
        {
            _handler = new MongoDBHandler(connectionString, databaseName);
            _handler.CreateDocument(collectionName, documentJson);
        }

        [OSAction(Description = "Retrieves documents from the specified collection as JSON.")]
        public string GetDocuments(string connectionString, string databaseName, string collectionName, string filterJson)
        {
            _handler = new MongoDBHandler(connectionString, databaseName);
            var bsonDocuments = _handler.GetDocuments(collectionName, filterJson);
            var jsonWriterSettings = new JsonWriterSettings { OutputMode = JsonOutputMode.Shell };
            return bsonDocuments.ToJson(jsonWriterSettings);
        }

        [OSAction(Description = "Update documents from the specified collection.")]
        public void UpdateDocument(string collectionName, string filterJson, string updateJson, string connectionString, string databaseName )
        {
            _handler = new MongoDBHandler(connectionString, databaseName);
            _handler.UpdateDocument(collectionName, filterJson, updateJson);
        }

        [OSAction(Description = "Delete documents from the specified collection.")]
        public void DeleteDocument(string collectionName, string filterJson, string connectionString, string databaseName)
        {
            _handler = new MongoDBHandler(connectionString, databaseName);
            _handler.DeleteDocument(collectionName, filterJson);
        }
    }
}