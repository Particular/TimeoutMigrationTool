namespace Particular.TimeoutMigrationTool.ASQ
{
    using System;
    using System.Security.Cryptography;
    using System.Text;

    public class DelayedDeliveryTableNameProvider : IProvideDelayedDeliveryTableName
    {
        public DelayedDeliveryTableNameProvider(string overriddenDelayedDeliveryTableName = null)
        {
            this.overriddenDelayedDeliveryTableName = overriddenDelayedDeliveryTableName;
        }

        public string GetDelayedDeliveryTableName(string endpointName)
        {
            if (!string.IsNullOrEmpty(overriddenDelayedDeliveryTableName))
            {
                return overriddenDelayedDeliveryTableName;
            }

            byte[] hashedName;
            using (var sha1 = SHA1.Create())
            {
                sha1.Initialize();
                hashedName = sha1.ComputeHash(Encoding.UTF8.GetBytes(endpointName));
            }

            var hashName = BitConverter.ToString(hashedName).Replace("-", string.Empty);
            return "delays" + hashName.ToLower();
        }

        public string GetStagingTableName(string endpointName)
        {
            return GetDelayedDeliveryTableName(endpointName) + "staging";
        }

        string overriddenDelayedDeliveryTableName;
    }
}