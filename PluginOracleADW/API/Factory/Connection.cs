using System.Data;
using System.Threading.Tasks;
using Oracle.ManagedDataAccess.Client;
using PluginOracleADW.Helper;

namespace PluginOracleADW.API.Factory
{
    public class Connection : IConnection
    {
        private static bool _settingsApplied = false;
        private readonly OracleConnection _conn;

        public Connection(Settings settings)
        {
            if (_settingsApplied == false)
            {
                OracleConfiguration.TnsAdmin = settings.WalletPath;
                OracleConfiguration.WalletLocation = settings.WalletPath;
                _settingsApplied = true;
            }

            _conn = new OracleConnection(settings.GetConnectionString());
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