using Microsoft.AspNetCore.Datasync.CosmosDb;

namespace api.Db;

public class TodoItem : CosmosTableData
{
    public string Title { get; set; } = "";

    public bool IsComplete { get; set; }
    public string UserId { get; set; }
}