using NUnit.Framework;

[SetUpFixture]
public class SetUpFixture
{
    [OneTimeSetUp]
    public void SetUp()
    {
        MsSqlMicrosoftDataClientConnectionBuilder.RecreateDbIfNotExists();
    }
}