using NUnit.Framework;
using Particular.TimeoutMigrationTool;

namespace TimeoutMigrationTool.Tests
{
    public class SqlDialectTests
    {
        [Test]
        public void SqlDialect_Parse_returns_MsSql()
        {
            var dialect = SqlDialect.Parse("Anything");

            Assert.IsTrue(dialect.GetType() == typeof(MsSqlServer));
        }
    }
}