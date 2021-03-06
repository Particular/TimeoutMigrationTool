﻿namespace Particular.TimeoutMigrationTool.NHibernate
{
    using global::NHibernate.Cfg;

    public class MsSqlDatabaseDialect : DatabaseDialect
    {
        public static string Name => "MsSqlServer";

        public override void ConfigureDriverAndDialect(Configuration configuration)
        {
            var properties = configuration.Properties;

            properties[Environment.ConnectionDriver] = "NHibernate.Driver.MicrosoftDataSqlClientDriver";
            properties[Environment.Dialect] = "NHibernate.Dialect.MsSql2008Dialect";
        }

        public override string GetSqlToBreakStagedTimeoutsIntoBatches(int batchSize)
        {
            return $@"
             UPDATE BatchMigration
                SET BatchMigration.BatchNumber = BatchMigration.CalculatedBatchNumber + 1
                FROM (
                    SELECT BatchNumber, (ROW_NUMBER() OVER (ORDER BY (select 0)) - 1) / {batchSize} AS CalculatedBatchNumber
                    FROM [StagedTimeoutEntity]
                ) BatchMigration;";
        }
    }
}