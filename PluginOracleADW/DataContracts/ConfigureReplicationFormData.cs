namespace PluginOracleADW.DataContracts
{
    public class ConfigureReplicationFormData
    {
        public string Owner { get; set; }
        public string GoldenTableName { get; set; }
        public string VersionTableName { get; set; }
    }
}