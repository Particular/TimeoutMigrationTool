namespace Particular.TimeoutMigrationTool.RavenDB
{
    public class RavenBatchInfo
    {
        public int Number { get; set; }

        public BatchState State { get; set; }

        public string[] TimeoutIds { get; set; }
    }
}
