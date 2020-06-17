namespace TimeoutMigrationTool.Tests
{
    using NUnit.Framework;
    using Particular.TimeoutMigrationTool.SqlP;

    public class SqlDialectTests
    {
        [Test]
        public void SqlDialect_Parse_returns_MsSql()
        {
            var dialect = Particular.TimeoutMigrationTool.SqlP.SqlDialect.Parse("Anything");

            Assert.IsTrue(dialect.GetType() == typeof(MsSqlServer));
        }
    }
}