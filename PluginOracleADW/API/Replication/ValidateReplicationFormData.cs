using System.Collections.Generic;
using PluginOracleADW.DataContracts;

namespace PluginOracleADW.API.Replication
{
    public static partial class Replication
    {
        public static List<string> ValidateReplicationFormData(this ConfigureReplicationFormData data)
        {
            var errors = new List<string>();

            if (string.IsNullOrWhiteSpace(data.Owner))
            {
                errors.Add("Schema name is empty.");
            }
            
            if (string.IsNullOrWhiteSpace(data.GoldenTableName))
            {
                errors.Add("Golden Record table name is empty.");
            }

            if (string.IsNullOrWhiteSpace(data.VersionTableName))
            {
                errors.Add("Version Record table name is empty.");
            }

            return errors;
        }
    }
}