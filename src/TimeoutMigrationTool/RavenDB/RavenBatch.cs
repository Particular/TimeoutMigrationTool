namespace Particular.TimeoutMigrationTool.RavenDB
{
    public class RavenBatch : BatchInfo
    {
        public RavenBatch(int number, BatchState state, int numberOfTimeouts) : base(number, state, numberOfTimeouts)
        {

        }

        public string[] TimeoutIds { get; set; }
    }
}
