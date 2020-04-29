using System.Collections.Generic;
using Newtonsoft.Json;

namespace PluginOracleADW.API.Replication
{
    public static partial class Replication
    {
        public static string GetUIJson()
        {
            var uiJsonObj = new Dictionary<string, object>
            {
                {"ui:order", new []
                {
                    // "Owner",
                    "GoldenTableName",
                    "VersionTableName"
                }}
            };

            return JsonConvert.SerializeObject(uiJsonObj);
        }
    }
}