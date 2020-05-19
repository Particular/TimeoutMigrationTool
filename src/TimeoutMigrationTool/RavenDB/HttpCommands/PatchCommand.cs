namespace Particular.TimeoutMigrationTool.RavenDB.HttpCommands
{
    class PatchCommand
    {
        public string Id { get; set; }
        public string Type { get; set; }
        public string ChangeVector { get; set; }
        public Patch Patch { get; set; }
    }
}