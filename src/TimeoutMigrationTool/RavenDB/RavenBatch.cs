namespace Particular.TimeoutMigrationTool.RavenDB
{
    using System;

    public class RavenBatch : BatchInfo
    {
        public RavenBatch(int number, BatchState state, int numberOfTimeouts) : base(number, state, numberOfTimeouts)
        {

        }

        public string[] TimeoutIds { get; set; }
        public DateTime CutoffDate { get; set; }
        public string EndpointName { get; set; }
    }
}
