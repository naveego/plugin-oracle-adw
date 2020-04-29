using System.Collections.Generic;
using Newtonsoft.Json;

namespace PluginOracleADW.API.Replication
{
    public static partial class Replication
    {
        public static string GetSchemaJson()
        {
            var schemaJsonObj = new Dictionary<string, object>
            {
                {"type", "object"},
                {"properties", new Dictionary<string, object>
                {
                    // {"Owner", new Dictionary<string, string>
                    // {
                    //     {"type", "string"},
                    //     {"title", "Owner"},
                    //     {"description", "Name of the Owner to put golden and version tables into in Oracle ADW"},
                    // }},
                    {"GoldenTableName", new Dictionary<string, string>
                    {
                        {"type", "string"},
                        {"title", "Golden Record Table Name"},
                        {"description", "Name for your golden record table in Oracle ADW"},
                    }},
                    {"VersionTableName", new Dictionary<string, string>
                    {
                        {"type", "string"},
                        {"title", "Version Record Bucket Name"},
                        {"description", "Name for your version record table in Oracle ADW"},
                    }},
                }},
                {"required", new []
                {
                    "Owner",
                    "GoldenTableName",
                    "VersionTableName"
                }}
            };
            
            return JsonConvert.SerializeObject(schemaJsonObj);
        }
    }
}