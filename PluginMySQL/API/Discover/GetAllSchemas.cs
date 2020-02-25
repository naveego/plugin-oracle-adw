using System;
using System.Collections.Generic;
using System.Linq;
using Naveego.Sdk.Plugins;
using PluginMySQL.API.Factory;
using PluginMySQL.Helper;

namespace PluginMySQL.API.Discover
{
    public static partial class Discover
    {
        private const string TableName = "TABLE_NAME";
        private const string TableSchema = "TABLE_SCHEMA";
        private const string TableType = "TABLE_TYPE";
        private const string ColumnName = "COLUMN_NAME";
        private const string DataType = "DATA_TYPE";
        private const string ColumnKey = "COLUMN_KEY";
        private const string IsNullable = "IS_NULLABLE";
        private const string CharacterMaxLength = "CHARACTER_MAXIMUM_LENGTH";

        private const string GetAllTablesAndColumnsQuery = @"
SELECT t.TABLE_NAME
     , t.TABLE_SCHEMA
     , t.TABLE_TYPE
     , c.COLUMN_NAME
     , c.DATA_TYPE
     , c.COLUMN_KEY
     , c.IS_NULLABLE
     , c.CHARACTER_MAXIMUM_LENGTH

FROM INFORMATION_SCHEMA.TABLES AS t
      INNER JOIN INFORMATION_SCHEMA.COLUMNS AS c ON c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME

WHERE t.TABLE_SCHEMA NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')

ORDER BY t.TABLE_NAME";

        public static async IAsyncEnumerable<Schema> GetAllSchemas(IConnectionFactory connFactory, int sampleSize = 5)
        {
            var conn = connFactory.GetConnection();
            await conn.OpenAsync();

            var cmd = connFactory.GetCommand(GetAllTablesAndColumnsQuery, conn);
            var reader = await cmd.ExecuteReaderAsync();

            Schema schema = null;
            var currentSchemaId = "";
            while (await reader.ReadAsync())
            {
                var schemaId = $"`{reader.GetValueById(TableSchema)}`.`{reader.GetValueById(TableName)}`";
                if (schemaId != currentSchemaId)
                {
                    // return previous schema
                    if (schema != null)
                    {
                        // add sample and count
                        var records = Read.Read.ReadRecords(connFactory, schema).Take(sampleSize);
                        schema.Sample.AddRange(await records.ToListAsync());
                        schema.Count = await GetCountOfRecords(connFactory, schema);
                        
                        yield return schema;
                    }

                    // start new schema
                    currentSchemaId = schemaId;
                    schema = new Schema
                    {
                        Id = currentSchemaId,
                        Name = schemaId,
                        Properties = { },
                        DataFlowDirection = Schema.Types.DataFlowDirection.Read
                    };
                }

                // add column to schema
                var property = new Property
                {
                    Id = $"`{reader.GetValueById(ColumnName)}`",
                    Name = reader.GetValueById(TableSchema).ToString(),
                    IsKey = reader.GetValueById(ColumnKey).ToString() == "PRI",
                    IsNullable = reader.GetValueById(IsNullable).ToString() == "YES",
                    Type = GetType(reader.GetValueById(DataType).ToString()),
                    TypeAtSource = GetTypeAtSource(reader.GetValueById(DataType).ToString(),
                        reader.GetValueById(CharacterMaxLength))
                };
                schema?.Properties.Add(property);
            }

            await conn.CloseAsync();
        }

        private static PropertyType GetType(string dataType)
        {
            switch (dataType)
            {
                case "datetime":
                case "timestamp":
                    return PropertyType.Datetime;
                case "date":
                    return PropertyType.Date;
                case "time":
                    return PropertyType.Time;
                case "tinyint":
                case "smallint":
                case "mediumint":
                case "int":
                    return PropertyType.Integer;
                case "decimal":
                case "bigint":
                    return PropertyType.Decimal;
                case "float":
                case "double":
                    return PropertyType.Float;
                case "boolean":
                    return PropertyType.Bool;
                case "blob":
                case "mediumblob":
                case "longblob":
                    return PropertyType.Blob;
                case "char":
                case "varchar":
                case "tinytext":
                    return PropertyType.String;
                case "text":
                case "mediumtext":
                case "longtext":
                    return PropertyType.Text;
                default:
                    return PropertyType.String;
            }
        }

        private static string GetTypeAtSource(string dataType, object maxLength)
        {
            return maxLength != null ? $"{dataType}({maxLength})" : dataType;
        }
    }
}