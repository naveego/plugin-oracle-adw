using System.Data;
using PluginOracleADW.Helper;

namespace PluginOracleADW.API.Factory
{
    public interface IConnectionFactory
    {
        void Initialize(Settings settings);
        IConnection GetConnection();
        IConnection GetConnection(string database);
        ICommand GetCommand(string commandText, IConnection conn);
    }
}