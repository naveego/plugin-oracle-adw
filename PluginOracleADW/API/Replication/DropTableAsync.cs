using System;
using System.Threading.Tasks;
using PluginOracleADW.API.Factory;
using PluginOracleADW.DataContracts;
using PluginOracleADW.Helper;

namespace PluginOracleADW.API.Replication
{
    public static partial class Replication
    {
        // private static readonly string DropTableQuery = @"DROP TABLE IF EXISTS {0}.{1}";
        private static readonly string DropTableQuery = @"
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE {0}.{1}';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -942 THEN
            RAISE;
        END IF;
END;";

        public static async Task DropTableAsync(IConnectionFactory connFactory, ReplicationTable table)
        {
            var conn = connFactory.GetConnection();
            await conn.OpenAsync();

            try
            {
                var cmd = connFactory.GetCommand(
                    string.Format(DropTableQuery,
                        Utility.Utility.GetSafeName(table.SchemaName),
                        Utility.Utility.GetSafeName(table.TableName)
                    ),
                    conn);
                await cmd.ExecuteNonQueryAsync();
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