// Copyright (c) Microsoft Corporation. All Rights Reserved.
// Licensed under the MIT License.

using Microsoft.Azure.Cosmos.Scripts;
using TestData = Datasync.Common.Test.TestData;

namespace Microsoft.AspNetCore.Datasync.CosmosDb.Test.Helpers;

[ExcludeFromCodeCoverage]
internal static class CosmosDbHelper
{
    internal static async Task<Container> GetContainer()
    {
        var databaseName = Guid.NewGuid().ToString("N");
        // Default emulator connection string, this is the same for everyone (https://learn.microsoft.com/en-us/azure/cosmos-db/emulator#authentication)
        var connectionString = "AccountEndpoint=https://localhost:8081/;AccountKey=C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
        var client = new CosmosClient(connectionString);
        Database database = await client.CreateDatabaseAsync(databaseName);
        Container container = await database.CreateContainerAsync("movies", "/Rating");

        var trigger = new TriggerProperties
        {
            Id = "CleanId",
            Body = @"function stripPartitionKeyFromId() {
  var context = getContext();
  var request = context.getRequest();
  var itemToCreate = request.getBody();
  if (itemToCreate.id) {
    var parts = itemToCreate.id.split("":"");
    if (parts.length > 1) {
      itemToCreate.id = parts[0];
    }
  }
  request.setBody(itemToCreate);
}",
            TriggerOperation = TriggerOperation.All,
            TriggerType = TriggerType.Pre,
        };

        await container.Scripts.CreateTriggerAsync(trigger); //.ConfigureAwait(false);

        // Populate with test data
        var seedData = Movies.OfType<CosmosMovie>();
        foreach (var movie in seedData)
        {
            var offset = -(180 + new Random().Next(180));
            movie.Rating ??= "NR";
            movie.Version = Guid.NewGuid().ToByteArray();
            movie.UpdatedAt = DateTimeOffset.UtcNow.AddDays(offset);
            await container.CreateItemAsync(movie);
        }
        return container;
    }
}
