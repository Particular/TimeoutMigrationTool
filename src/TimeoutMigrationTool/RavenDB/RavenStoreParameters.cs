namespace Particular.TimeoutMigrationTool.RavenDB
{
    using System;

    public class RavenStoreParameters
    {
        public DateTime MaxCutoffTime { get; set; } = DateTime.Now.AddDays(-1);
    }
}