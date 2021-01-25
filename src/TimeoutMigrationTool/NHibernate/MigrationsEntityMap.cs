namespace Particular.TimeoutMigrationTool.NHibernate
{
    using global::NHibernate.Mapping.ByCode.Conformist;
    using global::NHibernate.Type;

    class MigrationsEntityMap : ClassMapping<MigrationsEntity>
    {
        public MigrationsEntityMap()
        {
            Id(x => x.MigrationRunId);
            Property(p => p.EndpointName, pm => { pm.Length(440); });
            Property(p => p.Status, pm => pm.Type<EnumType<MigrationStatus>>());
            Property(p => p.RunParameters, pm => { pm.Length(4001); });
            Property(p => p.NumberOfBatches);
            Property(p => p.CutOffTime);
            Property(p => p.StartedAt);
            Property(p => p.CompletedAt);
        }
    }
}