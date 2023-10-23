using Microsoft.Azure.Cosmos;
using System;
using System.Collections.Generic;

namespace Microsoft.AspNetCore.Datasync.CosmosDb;

/// <summary>
/// Defines all the options that can be used to configure a CosmosRepository.
/// </summary>
public class CosmosRepositoryOptions
{
    /// <summary>
    /// Gets or sets the names of the partition keys to use. If null then "/id" is used.
    /// </summary>
    public List<string> PartitionKeyPropertyNames { get; set; }

    /// <summary>
    /// Gets or sets the <see cref="ItemRequestOptions"/> to use when accessing the database.
    /// </summary>
    public ItemRequestOptions ItemRequestOptions { get; set; } = new();

    /// <summary>
    /// Gets or sets delegate to be used to parse the ID and partition key from a string. If null, then the default parser will be used
    /// which assume the ID to be in form of: "id:partitionKey" for a single partition key or "id:pk1|pk2|pk3" for a hierarchical key.
    /// </summary>
    public Func<string, (string id, PartitionKey partitionKey)> ParseIdAndPartitionKey { get; set; }
}