using System;
using System.Threading.Tasks;
using Naveego.Sdk.Logging;
using PluginOracleADW.API.Factory;
using PluginOracleADW.DataContracts;
using PluginOracleADW.Helper;

namespace PluginOracleADW.API.Replication
{
    public static partial class Replication
    {
        private static readonly string RecordExistsQuery = @"SELECT COUNT(*) c
FROM (
SELECT * FROM {0}.{1}
WHERE {2} = '{3}'    
) q";

        public static async Task<bool> RecordExistsAsync(IConnectionFactory connFactory, ReplicationTable table,
            string primaryKeyValue)
        {
            var conn = connFactory.GetConnection();
            await conn.OpenAsync();

            try
            {
                var cmd = connFactory.GetCommand(string.Format(RecordExistsQuery,
                        Utility.Utility.GetSafeName(table.SchemaName),
                        Utility.Utility.GetSafeName(table.TableName),
                        Utility.Utility.GetSafeName(table.Columns.Find(c => c.PrimaryKey).ColumnName),
                        primaryKeyValue
                    ),
                    conn);

                // check if record exists
                var reader = await cmd.ExecuteReaderAsync();
                await reader.ReadAsync();
                var count = (decimal) reader.GetValueById("c");
            
                return count != 0;
            }
            catch (Exception e)
            {
                Logger.Error(e, e.Message);
                throw;
            }
            finally
            {
                await conn.CloseAsync();
            }
        }
    }
}