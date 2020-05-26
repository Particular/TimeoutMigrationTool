using System;

namespace Particular.TimeoutMigrationTool.RavenDB
{
    internal static class RavenDataReaderFactory
    {
        public static ICanTalkToRavenVersion Resolve(string serverUrl, string databaseName, RavenDbVersion version)
        {
            switch (version)
            {
                case RavenDbVersion.ThreeDotFive:
                    return new Raven3Adapter(serverUrl, databaseName);
                case RavenDbVersion.Four:
                    return new Raven4Adapter(serverUrl, databaseName);
                default:
                    throw new ArgumentOutOfRangeException(nameof(version), version, null);
            }
        }
    }
}