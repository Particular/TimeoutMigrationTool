namespace TimeoutMigrationTool.ASQ.IntegrationTests
{
    using NUnit.Framework;
    using Particular.TimeoutMigrationTool.ASQ;

    [TestFixture]
    class DelayedDeliveryTableNameProviderTests
    {
        [Test]
        public void When_Migrating_Endpoint_With_No_DelayedDeliveryTableOverride_DelayedDeliveryTableName_Is_Computed()
        {
            // Arrange
            var delayedDeliveryTableNameGenerator = new DelayedDeliveryTableNameProvider(null);

            // Act
            var delayedDeliveryTableName = delayedDeliveryTableNameGenerator.GetDelayedDeliveryTableName("EndpointName");

            // Assert
            Assert.That(delayedDeliveryTableName.StartsWith("delays"), Is.True);
        }

        [Test]
        public void When_Migrating_Endpoint_With_DelayedDeliveryTableOverride_DelayedDeliveryTableName_Is_NotComputed()
        {
            // Arrange
            var delayedDeliveryTableNameGenerator = new DelayedDeliveryTableNameProvider("overriden delayed delivery table name");

            // Act
            var delayedDeliveryTableName = delayedDeliveryTableNameGenerator.GetDelayedDeliveryTableName("EndpointName");

            // Assert
            Assert.AreEqual("overriden delayed delivery table name", delayedDeliveryTableName);
        }
    }
}
