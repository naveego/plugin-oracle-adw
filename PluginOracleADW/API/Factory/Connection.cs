using System.Data;
using System.Threading.Tasks;
using Oracle.ManagedDataAccess.Client;
using PluginOracleADW.Helper;

namespace PluginOracleADW.API.Factory
{
    public class Connection : IConnection
    {
        private readonly OracleConnection _conn;
        private readonly Settings _settings;

        public Connection(Settings settings)
        {
            //OracleConfiguration.SqlNetWalletOverride = true;
            OracleConfiguration.TnsAdmin = settings.WalletPath;
            OracleConfiguration.WalletLocation = settings.WalletPath;
            OracleConfiguration.TraceFileLocation = "/tmp/traces";
            OracleConfiguration.TraceLevel = 7;
            OracleConfiguration.TraceOption = 1;
            _conn = new OracleConnection(settings.GetConnectionString());

            _settings = settings;
        }

        public async Task OpenAsync()
        {
            await _conn.OpenAsync();
        }

        public async Task CloseAsync()
        {
            await _conn.CloseAsync();
        }

        public Task<bool> PingAsync()
        {
            return Task.FromResult(true);
        }

        public IDbConnection GetConnection()
        {
            return _conn;
        }
    }
}