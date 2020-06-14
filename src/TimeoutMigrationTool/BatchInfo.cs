namespace Particular.TimeoutMigrationTool
{
    public class BatchInfo
    {
        public int Number { get; set; }
        public BatchState State { get; set; }
        public string[] TimeoutIds { get; set; }
        public int NumberOfTimeouts { get; set; }
    }
}