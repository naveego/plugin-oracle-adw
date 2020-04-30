using System.Collections.Generic;
using Naveego.Sdk.Plugins;
using PluginOracleADW.DataContracts;

namespace PluginOracleADW.API.Replication
{
    public static partial class Replication 
    {
        public static ReplicationTable ConvertSchemaToReplicationTable(Schema schema, string schemaName,
            string tableName)
        {
            var table = new ReplicationTable
            {
                SchemaName = schemaName,
                TableName = tableName,
                Columns = new List<ReplicationColumn>()
            };
            
            foreach (var property in schema.Properties)
            {
                var column = new ReplicationColumn
                {
                    ColumnName = property.Name,
                    DataType = string.IsNullOrWhiteSpace(property.TypeAtSource)? GetType(property.Type): property.TypeAtSource,
                    PrimaryKey = false
                };
                
                table.Columns.Add(column);
            }

            return table;
        }
        
        private static string GetType(PropertyType dataType)
        {
            switch (dataType)
            {
                case PropertyType.Datetime:
                    return "VARCHAR(255)";
                case PropertyType.Date:
                    return "VARCHAR(255)";
                case PropertyType.Time:
                    return "VARCHAR(255)";
                case PropertyType.Integer:
                    return "NUMBER";
                case PropertyType.Decimal:
                    return "DECIMAL";
                case PropertyType.Float:
                    return "BINARY_DOUBLE";
                case PropertyType.Bool:
                    return "BOOLEAN";
                case PropertyType.Blob:
                    return "BLOB";
                case PropertyType.String:
                    return "VARCHAR(255)";
                case PropertyType.Text:
                    return "CLOB";
                default:
                    return "CLOB";
            }
        }
    }
}