using Microsoft.AspNetCore.Datasync;
using Microsoft.AspNetCore.Datasync.CosmosDb;
using Newtonsoft.Json;
using System.Text;

namespace api.Db;

public class TodoItem : CosmosTableData
{ 
    public bool IsComplete { get; set; }

    public string Title { get; set; } = "";

    public string UserId { get; set; }

    protected override string TranslateToId()
    {
        return $"{LookupId}:{UserId}";
    }
}