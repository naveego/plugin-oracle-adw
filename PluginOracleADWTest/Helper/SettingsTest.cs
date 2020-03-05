using System;
using System.Collections.Generic;
using PluginOracleADW.Helper;
using Xunit;

namespace PluginOracleADWTest.Helper
{
    public class SettingsTest
    {
        [Fact]
        public void ValidateValidTest()
        {
            // setup
            var settings = new Settings
            {
                WalletPath = "/Users/derek/Downloads/Wallet_NaveegoAW",
                TNSName = "NaveegoAW_MEDIUM"
            };

            // act
            settings.Validate();

            // assert
        }

        [Fact]
        public void ValidateNoTNSNameTest()
        {
            // setup
            var settings = new Settings
            {
                WalletPath = "/home/user/wallet",
                TNSName = null
            };

            // act
            Exception e = Assert.Throws<Exception>(() => settings.Validate());

            // assert
            Assert.Contains("The TNSName property must be set", e.Message);
        }
        
        [Fact]
        public void ValidateNoWalletPathTest()
        {
            // setup
            var settings = new Settings
            {
                WalletPath = null,
                TNSName = "Naveego"
            };

            // act
            Exception e = Assert.Throws<Exception>(() => settings.Validate());

            // assert
            Assert.Contains("The WalletPath property must be set", e.Message);
        }

        
        
        [Fact]
        public void GetConnectionStringTest()
        {
            // setup
            var settings = new Settings
            {
                WalletPath = "/home/user/wallet",
                TNSName = "testtns"
            };

            // act
            var connString = settings.GetConnectionString();

            // assert
            Assert.Equal("testtns", connString);
        }
    }
}