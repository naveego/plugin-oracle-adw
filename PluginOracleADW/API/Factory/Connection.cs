using System.Data;
using System.Threading.Tasks;
using Oracle.ManagedDataAccess.Client;
using PluginOracleADW.Helper;

namespace PluginOracleADW.API.Factory
{
    public class Connection : IConnection
    {
        private readonly OracleConnection _conn;

        public Connection(Settings settings)
        {
            _conn = new OracleConnection(settings.GetConnectionString());
        }

        public Connection(Settings settings, string database)
        {
            _conn = new OracleConnection(settings.GetConnectionString(database));
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