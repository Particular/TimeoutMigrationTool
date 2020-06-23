namespace Particular.TimeoutMigrationTool.RavenDB
{
    using System;

    public class RavenConstants
    {
        public const string ToolStateId = "TimeoutMigrationTool/State";
        public const string ArchivedToolStateIdPrefix = "TimeoutMigrationTool/MigrationRun-";
        public const string TimeoutIndexName = "TimeoutsIndex";
        public const int MaxUriLength = 2000;
        public const int MaxNrOfDocumentsToRetrieve = 2048;
        public const int DefaultPagingSize = 1024;
        public const string MigrationOngoingPrefix = "__hidden__";
        public const string MigrationDonePrefix = "__migrated__";
        public const string DefaultTimeoutPrefix  = "TimeoutDatas";
        public const string BatchPrefix = "batch";

        public static int GetMaxNrOfTimeoutsWithoutIndexByRavenVersion(RavenDbVersion version)
        {
            switch (version)
            {
                case RavenDbVersion.ThreeDotFive:
                    return 300000;
                case RavenDbVersion.Four:
                    return 1000000;
                default:
                    throw new ArgumentOutOfRangeException("Unsupported version of RavenDB");
            }
        }
    }
}