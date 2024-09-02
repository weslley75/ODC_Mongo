using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;


namespace MongoDB_ODC
{
    public class MongoService
    {
        private readonly IMongoDatabase _database;

        public MongoService(string connectionString, string databaseName)
        {
            var client = new MongoClient(connectionString);
            _database = client.GetDatabase(databaseName);
        }

        public bool ValidateConnection()
        {
            try
            {
                _database.RunCommandAsync((Command<BsonDocument>)"{ping:1}").Wait();
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to connect to database: {ex.Message}");
                return false;
            }
        }

        public IMongoCollection<BsonDocument> GetCollection(string collectionName)
        {
            return _database.GetCollection<BsonDocument>(collectionName);
        }

        public bool CollectionExists(string collectionName)
        {
            var filter = new BsonDocument("name", collectionName);
            var collections = _database.ListCollections(new ListCollectionsOptions { Filter = filter });
            return collections.Any();
        }

        public long GetDocumentCount(string collectionName)
        {
            var collection = GetCollection(collectionName);
            return collection.CountDocuments(new BsonDocument());
        }

        //public List<BsonDocument> AggregateCollection(string collectionName, IEnumerable<BsonDocument> pipeline)
        //{
        //    var collection = GetCollection(collectionName);
        //    return collection.Aggregate<BsonDocument>((PipelineDefinition<BsonDocument, BsonDocument>)pipeline).ToList();
        //}

        public List<BsonDocument> AggregateCollection(string collectionName, IEnumerable<BsonDocument> pipelineDocuments)
        {
            var collection = _database.GetCollection<BsonDocument>(collectionName);

            try
            {
                // Iniciando o Aggregate Fluent
                var fluent = collection.Aggregate();

                // Aplicando cada estágio do pipeline
                foreach (var stage in pipelineDocuments)
                {
                    fluent = fluent.AppendStage<BsonDocument>(stage);
                }

                // Executando a agregação
                var results = fluent.ToList();

                Console.WriteLine($"Documentos agregados fluentemente: {results.Count}");
                return results;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro durante a agregação fluent: {ex.Message}");
                return new List<BsonDocument>(); // Retorna lista vazia ou gerencia o erro conforme necessário
            }
        }
}
}
