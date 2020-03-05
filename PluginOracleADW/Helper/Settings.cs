using System;
using System.Collections.Generic;

namespace PluginOracleADW.Helper
{
    public class Settings
    {
        public string Username { get; set; }
        
        public string Password { get; set; }

        public string WalletPath { get; set; }
        
        public string TNSName { get; set; }

        /// <summary>
        /// Validates the settings input object
        /// </summary>
        /// <exception cref="Exception"></exception>
        public void Validate()
        {
            if (String.IsNullOrEmpty(TNSName))
            {
                throw new Exception("The TNSName property must be set");
            }
        }

        /// <summary>
        /// Gets the database connection string
        /// </summary>
        /// <returns></returns>
        public string GetConnectionString()
        {
            return $"User Id={Username};Password={Password};Data Source={TNSName};Pooling=false";
        }
        
        /// <summary>
        /// Gets the database connection string
        /// </summary>
        /// <returns></returns>
        //public string GetConnectionString(string database)
        //{
            //return $"${TNSName}";
        //}
    }
}