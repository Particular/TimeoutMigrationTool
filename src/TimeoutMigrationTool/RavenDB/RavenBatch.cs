namespace Particular.TimeoutMigrationTool.RavenDB
{
    public class RavenBatch
    {
        public int Number { get; set; }

        public BatchState State { get; set; }

        public string[] TimeoutIds { get; set; }
    }
}
