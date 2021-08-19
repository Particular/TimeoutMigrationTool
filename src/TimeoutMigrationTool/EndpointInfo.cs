namespace Particular.TimeoutMigrationTool
{
    using System;
    using System.Collections.Generic;

    public class EndpointInfo
    {
        public string EndpointName { get; set; }
        public int NrOfTimeouts { get; set; }
        public DateTimeOffset LongestTimeout { get; set; }
        public DateTimeOffset ShortestTimeout { get; set; }
        public IList<string> Destinations { get; set; }
    }
}