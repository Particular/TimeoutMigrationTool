namespace TimeoutMigrationTool.Raven.FakeData
{
    using Particular.TimeoutMigrationTool;

    class Raven4InsertCommand
    {
        public string Id { get; set; }
        public string Type { get; set; }
        public object ChangeVector { get; set; }
        public TimeoutData Document { get; set; }
    }
}