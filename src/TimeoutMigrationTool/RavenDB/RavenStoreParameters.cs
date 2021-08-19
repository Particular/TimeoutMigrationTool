namespace Particular.TimeoutMigrationTool.RavenDB
{
    using System;

    public class RavenStoreParameters
    {
        public DateTimeOffset MaxCutoffTime { get; set; } = DateTimeOffset.UtcNow.AddDays(-1);
    }
}