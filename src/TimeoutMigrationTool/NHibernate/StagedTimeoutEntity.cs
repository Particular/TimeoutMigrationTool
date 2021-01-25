namespace Particular.TimeoutMigrationTool.NHibernate
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Runtime.Serialization.Json;
    using System.Text;

    public class StagedTimeoutEntity
    {
        public virtual Guid Id { get; set; }
        public virtual string Destination { get; set; }
        public virtual Guid SagaId { get; set; }
        public virtual byte[] State { get; set; }
        public virtual string Endpoint { get; set; }
        public virtual DateTime Time { get; set; }
        public virtual string Headers { get; set; }
        public virtual int BatchNumber { get; set; }
        public virtual BatchState BatchState { get; set; }

        public virtual TimeoutData ToTimeoutData()
        {
            return new TimeoutData
            {
                Id = Id.ToString(),
                Destination = Destination,
                Headers = ConvertStringToDictionary(Headers),
                OwningTimeoutManager = null,
                SagaId = SagaId,
                State = State,
                Time = Time
            };
        }

        static Dictionary<string, string> ConvertStringToDictionary(string data)
        {
            return string.IsNullOrEmpty(data) ? new Dictionary<string, string>() : DeSerialize<Dictionary<string, string>>(data);
        }

        private static T DeSerialize<T>(string data)
        {
            using var stream = new MemoryStream(Encoding.UTF8.GetBytes(data));
            return (T)Serializer<T>.Instance.ReadObject(stream);
        }

        private static class Serializer<T>
        {
            public static readonly DataContractJsonSerializer Instance = new DataContractJsonSerializer(typeof(T), new DataContractJsonSerializerSettings
            {
                UseSimpleDictionaryFormat = true,
            });
        }
    }
}