namespace TimeoutMigrationTool.Raven.FakeData
{
    using System.Collections.Generic;

    public class DatabaseRecordRaven3
    {
        public DatabaseRecordRaven3(string databaseName)
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