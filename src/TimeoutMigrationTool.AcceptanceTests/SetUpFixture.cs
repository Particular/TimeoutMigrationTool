using NUnit.Framework;

[SetUpFixture]
public class SetUpFixture
{
    [OneTimeSetUp]
    public void SetUp()
    {
        MsSqlMicrosoftDataClientHelper.RecreateDbIfNotExists();
    }
}