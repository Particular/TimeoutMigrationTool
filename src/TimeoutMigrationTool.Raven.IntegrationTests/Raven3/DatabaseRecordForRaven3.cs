namespace TimeoutMigrationTool.Raven.IntegrationTests.Raven3
{
    using System.Collections.Generic;

    public class DatabaseRecordForRaven3
    {
        public DatabaseRecordForRaven3(string databaseName)
        {
            Settings = new Dictionary<string, string>
            {
                { "Raven/ActiveBundles", "" },
                { "Raven/DataDir", $"~/{databaseName}" }
            };
        }
        public string SecuredSettings { get; set; }
        public bool Disabled { get; set; }

        public Dictionary<string, string> Settings { get; set; }
    }
}