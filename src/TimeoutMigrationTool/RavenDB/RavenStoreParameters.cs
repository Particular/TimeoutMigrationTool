using System;

namespace Particular.TimeoutMigrationTool.RavenDB
{
    public class RavenStoreParameters
    {
        public DateTime MaxCutoffTime { get; set; } = DateTime.Now.AddDays(-1);
    }
}