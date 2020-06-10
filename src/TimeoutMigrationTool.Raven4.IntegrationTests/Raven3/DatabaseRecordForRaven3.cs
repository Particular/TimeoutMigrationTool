namespace TimeoutMigrationTool.Raven4.IntegrationTests.Raven3
{
    using System.Collections.Generic;

    public class DatabaseRecordForRaven3
    {
        public DatabaseRecordForRaven3(string databaseName)
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