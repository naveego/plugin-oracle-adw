using System.Collections.Generic;
using Google.Protobuf.Collections;
using Naveego.Sdk.Plugins;
using PluginMySQL.API.Factory;

namespace PluginMySQL.API.Discover
{
    public static partial class Discover
    {
        public static async IAsyncEnumerable<Schema> GetRefreshSchemas(IConnectionFactory connFactory,
            RepeatedField<Schema> refreshSchemas)
        {
            foreach (var schema in refreshSchemas)
            {
                if (string.IsNullOrWhiteSpace(schema.Query))
                {
                    yield return await GetSchemaForTable(connFactory, schema);
                }
                
                
            }
        }
    }
}