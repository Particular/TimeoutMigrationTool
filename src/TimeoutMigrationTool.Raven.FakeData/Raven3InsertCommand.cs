namespace TimeoutMigrationTool.Raven.FakeData
{
    using Particular.TimeoutMigrationTool;

    public class Raven3InsertCommand
    {
        public string Key { get; set; }
        public string Method { get; set; }
        public TimeoutData Document { get; set; }
        public object MetaData { get; set; }
    }
}