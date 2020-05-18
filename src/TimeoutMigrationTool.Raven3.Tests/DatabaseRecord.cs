using System.Collections.Generic;

namespace TimeoutMigrationTool.Raven3.Tests
{
    public class DatabaseRecord
    {
        public DatabaseRecord(string databaseName)
        {
            Settings = new Dictionary<string, string>();
            Settings.Add("Raven/ActiveBundles", "");
            Settings.Add("Raven/DataDir", $"~/{databaseName}");
        }
        public string SecuredSettings { get; set; }
        public bool Disabled { get; set; }

        public Dictionary<string, string> Settings { get; set; }
    }
}