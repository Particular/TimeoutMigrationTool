namespace TimeoutMigrationTool.Raven.IntegrationTests
{
    using Particular.TimeoutMigrationTool.RavenDB;

    public static class IRavenTestSuiteExtensions
    {
        public static RavenToolStateDto FromToolState(this IRavenTestSuite suite, RavenToolState toolState)
        {
            return new RavenToolStateDto
            {
                RunParameters = toolState.RunParameters,
                Batches = RavenToolStateDto.ToBatches(toolState.Batches),
                Endpoint = toolState.EndpointName,
                Status = toolState.Status
            };
        }
    }
}