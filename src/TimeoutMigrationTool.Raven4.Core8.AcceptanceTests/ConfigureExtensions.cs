namespace TimeoutMigrationTool.Raven4.Core8.AcceptanceTests
{
    using Microsoft.Extensions.DependencyInjection;
    using NServiceBus;
    using NServiceBus.AcceptanceTesting.Support;

    public static class ConfigureExtensions
    {
        public static void RegisterComponentsAndInheritanceHierarchy(this EndpointConfiguration builder, RunDescriptor runDescriptor)
        {
            builder.RegisterComponents(r => { RegisterInheritanceHierarchyOfContextOnContainer(runDescriptor, r); });
        }

        static void RegisterInheritanceHierarchyOfContextOnContainer(RunDescriptor runDescriptor, IServiceCollection sc)
        {
            var type = runDescriptor.ScenarioContext.GetType();
            while (type != typeof(object))
            {
                sc.AddSingleton(type, runDescriptor.ScenarioContext);
                type = type.BaseType;
            }
        }
    }
}