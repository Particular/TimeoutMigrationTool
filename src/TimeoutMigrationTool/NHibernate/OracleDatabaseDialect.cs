namespace Particular.TimeoutMigrationTool.NHibernate
{
    using global::NHibernate.Cfg;

    public class OracleDatabaseDialect : DatabaseDialect
    {
        public static string Name => "Oracle";

        public override void ConfigureDriverAndDialect(Configuration configuration)
        {
            var properties = configuration.Properties;

            properties[Environment.ConnectionDriver] = "NHibernate.Driver.OracleManagedDataClientDriver";
            properties[Environment.Dialect] = "NHibernate.Dialect.Oracle10gDialect";
        }

        public override string GetSqlToBreakStagedTimeoutsIntoBatches(int batchSize)
        {
            return $@"MERGE INTO StagedTimeoutEntity STE
USING (SELECT Id, CAST (ROW_NUMBER() OVER (ORDER BY Id) / {batchSize} AS INT) calculatedBatchNumber
FROM StagedTimeoutEntity) SRC
ON (SRC.Id = STE.Id)
WHEN MATCHED THEN UPDATE SET BatchNumber = calculatedBatchNumber";
        }
    }
}