namespace Particular.TimeoutMigrationTool.RavenDB
{
    using System;

    static class RavenDataReaderFactory
    {
        public static ICanTalkToRavenVersion Resolve(string serverUrl, string databaseName, RavenDbVersion version)
        {
            return version switch
            {
                RavenDbVersion.ThreeDotFive => new Raven3Adapter(serverUrl, databaseName),
                RavenDbVersion.Four => new Raven4Adapter(serverUrl, databaseName),
                _ => throw new ArgumentOutOfRangeException(nameof(version), version, null),
            };
        }
    }
}