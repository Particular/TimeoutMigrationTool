namespace TimeoutMigrationTool.Raven.IntegrationTests
{
    using System;

    public class InitiTimeoutsResult
    {
        public DateTimeOffset ShortestTimeout { get; set; }
        public DateTimeOffset LongestTimeout { get; set; }
    }
}