namespace Particular.TimeoutMigrationTool.RavenDB
{
    using Particular.TimeoutMigrationTool.RavenDB.HttpCommands;

    class Raven3BatchCommand
    {
        public string Key { get; set; }
        public string Method { get; set; }
        public bool DebugMode { get; set; }
        public Patch Patch { get; set; }
    }
}