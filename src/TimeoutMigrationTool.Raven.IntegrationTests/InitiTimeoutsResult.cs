namespace TimeoutMigrationTool.Raven.IntegrationTests
{
    using System;

    public class InitiTimeoutsResult
    {
        public DateTime ShortestTimeout { get; set; }
        public DateTime LongestTimeout { get; set; }
    }
}