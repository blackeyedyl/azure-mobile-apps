using api.Db;
using Microsoft.AspNetCore.Datasync.CosmosDb;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Options;
using System.ComponentModel;

namespace api.Extensions;

public static class AspNetCoreExtensions
{
    public static IServiceCollection AddCosmosDatasync(this IServiceCollection services, string connectionString, string databaseName)
    {
        CosmosClientOptions options = new()
        {
            Serializer = new CosmosDatasyncSerializer()
        };
        var client = new CosmosClient(connectionString, options);
        var database = client.GetDatabase(databaseName);
        var todoItemsContainer = database.GetContainer("TodoItems");
        services.AddSingleton(new CosmosTableRepository<TodoItem>(todoItemsContainer, new() { PartitionKeyPropertyNames = new() { "UserId" } }));

        return services;
    }
}
