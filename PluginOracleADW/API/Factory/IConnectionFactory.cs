using System.Data;
using PluginOracleADW.Helper;

namespace PluginOracleADW.API.Factory
{
    public interface IConnectionFactory
    {
        void Initialize(Settings settings);
        IConnection GetConnection();
        ICommand GetCommand(string commandText, IConnection conn);
    }
}