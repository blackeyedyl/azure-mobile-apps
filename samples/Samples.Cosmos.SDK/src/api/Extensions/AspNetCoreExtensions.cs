using api.Db;
using Microsoft.AspNetCore.Datasync.CosmosDb;
using Microsoft.Azure.Cosmos;
using System.ComponentModel;

namespace api.Extensions;

public static class AspNetCoreExtensions
{
    public static IServiceCollection AddCosmosDatasync(this IServiceCollection services, string connectionString, string databaseName)
    {
        var client = new CosmosClient(connectionString);
        var database = client.GetDatabase(databaseName);
        var todoItemsContainer = database.GetContainer("TodoItems");
        var requestOptions = new ItemRequestOptions
        {
            PreTriggers = new List<string> { "CleanId" }
        };
        services.AddSingleton(new CosmosTableRepository<TodoItem>(todoItemsContainer, new() { "UserId" }, requestOptions));

        return services;
    }
}
