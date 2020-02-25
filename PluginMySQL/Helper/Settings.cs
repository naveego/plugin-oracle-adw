using System;
using System.Collections.Generic;

namespace PluginMySQL.Helper
{
    public class Settings
    {
        public string Server { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }
        public string Database { get; set; }

        /// <summary>
        /// Validates the settings input object
        /// </summary>
        /// <exception cref="Exception"></exception>
        public void Validate()
        {
            if (String.IsNullOrEmpty(Server))
            {
                throw new Exception("The Server property must be set");
            }
            
            if (String.IsNullOrEmpty(Database))
            {
                throw new Exception("The Database property must be set");
            }

            if (String.IsNullOrEmpty(Username))
            {
                throw new Exception("The Username property must be set");
            }
            
            if (String.IsNullOrEmpty(Password))
            {
                throw new Exception("The Password property must be set");
            }
        }

        /// <summary>
        /// Gets the database connection string
        /// </summary>
        /// <returns></returns>
        public string GetConnectionString()
        {
            return $"server={Server};user={Username};password={Password};database={Database}";
        }
        
        /// <summary>
        /// Gets the database connection string
        /// </summary>
        /// <returns></returns>
        public string GetConnectionString(string database)
        {
            return $"server={Server};user={Username};password={Password};database={database}";
        }
    }
}