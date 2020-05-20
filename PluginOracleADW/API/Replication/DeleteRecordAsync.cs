using System;
using System.Threading.Tasks;
using PluginOracleADW.API.Factory;
using PluginOracleADW.DataContracts;
using PluginOracleADW.Helper;

namespace PluginOracleADW.API.Replication
{
    public static partial class Replication
    {
        private static readonly string DeleteRecordQuery = @"DELETE FROM {0}.{1}
WHERE {2} = '{3}'";

        public static async Task DeleteRecordAsync(IConnectionFactory connFactory, ReplicationTable table,
            string primaryKeyValue)
        {
            var conn = connFactory.GetConnection();
            await conn.OpenAsync();

            try
            {
                var cmd = connFactory.GetCommand(string.Format(DeleteRecordQuery,
                        Utility.Utility.GetSafeName(table.SchemaName),
                        Utility.Utility.GetSafeName(table.TableName),
                        Utility.Utility.GetSafeName(table.Columns.Find(c => c.PrimaryKey).ColumnName),
                        primaryKeyValue
                    ),
                    conn);

                // check if table exists
                await cmd.ExecuteNonQueryAsync();
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                throw;
            }
            finally
            {
                await conn.CloseAsync();
            }
        }
    }
}