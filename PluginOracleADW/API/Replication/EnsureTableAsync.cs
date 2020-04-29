using System;
using System.Text;
using System.Threading.Tasks;
using PluginOracleADW.API.Factory;
using PluginOracleADW.DataContracts;
using PluginOracleADW.Helper;

namespace PluginOracleADW.API.Replication
{
    public static partial class Replication
    {
        private static readonly string EnsureTableQuery = @"SELECT COUNT(*) c
FROM ALL_TABLES 
WHERE OWNER = '{0}' 
AND TABLE_NAME = '{1}'";
        
        // private static readonly string EnsureTableQuery = @"SELECT * FROM {0}.{1}";

        public static async Task EnsureTableAsync(IConnectionFactory connFactory, ReplicationTable table)
        {
            var conn = connFactory.GetConnection();
            await conn.OpenAsync();
            
            // Logger.Info($"Creating Schema... {table.SchemaName}");
            // var cmd = connFactory.GetCommand($"CREATE SCHEMA IF NOT EXISTS {table.SchemaName}", conn);
            // await cmd.ExecuteNonQueryAsync();

            var cmd = connFactory.GetCommand(string.Format(EnsureTableQuery, table.SchemaName, table.TableName), conn);
            
            Logger.Debug($"Creating Table: {string.Format(EnsureTableQuery, table.SchemaName, table.TableName)}");

            // check if table exists
            var reader = await cmd.ExecuteReaderAsync();
            await reader.ReadAsync();
            var count = (decimal)reader.GetValueById("c");
            await conn.CloseAsync();
            
            if (count == 0)
            {
                // create table
                var querySb = new StringBuilder($@"CREATE TABLE 
{Utility.Utility.GetSafeName(table.SchemaName)}.{Utility.Utility.GetSafeName(table.TableName)} (");
                var primaryKeySb = new StringBuilder("PRIMARY KEY (");
                var hasPrimaryKey = false;
                foreach (var column in table.Columns)
                {
                    querySb.Append(
                        $"{Utility.Utility.GetSafeName(column.ColumnName)} {column.DataType},");
                    if (column.PrimaryKey)
                    {
                        primaryKeySb.Append($"{Utility.Utility.GetSafeName(column.ColumnName)},");
                        hasPrimaryKey = true;
                    }
                }

                if (hasPrimaryKey)
                {
                    primaryKeySb.Length--;
                    primaryKeySb.Append(")");
                    querySb.Append($"{primaryKeySb})");
                }
                else
                {
                    querySb.Length--;
                    querySb.Append(")");
                }

                var query = querySb.ToString();
                Logger.Debug($"Creating Table: {query}");
                
                await conn.OpenAsync();

                cmd = connFactory.GetCommand(query, conn);

                await cmd.ExecuteNonQueryAsync();
                await conn.CloseAsync();
            }
        }
    }
}