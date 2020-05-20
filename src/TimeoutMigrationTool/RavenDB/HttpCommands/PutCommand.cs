namespace Particular.TimeoutMigrationTool.RavenDB.HttpCommands
{
    class PutCommand
    {
        public string Id { get; set; }
        public string Type { get; set; }
        public object ChangeVector { get; set; }
        public BatchInfo Document { get; set; }
    }
}