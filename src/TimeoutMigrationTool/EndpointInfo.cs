namespace Particular.TimeoutMigrationTool
{
    using System;

    public class EndpointInfo
    {
        public string EndpointName { get; set; }
        public int NrOfTimeouts { get; set; }
        public DateTime LongestTimeout { get; set; }
        public DateTime ShortestTimeout { get; set; }
    }
}