using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization.Json;
using System.Text;

namespace Particular.TimeoutMigrationTool.Nhb
{
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
            if (string.IsNullOrEmpty(data))
            {
                return new Dictionary<string, string>();
            }

            return DeSerialize<Dictionary<string, string>>(data);
        }

        public static T DeSerialize<T>(string data)
        {
            var serializer = BuildSerializer(typeof(T));
            using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(data)))
            {
                return (T)serializer.ReadObject(stream);
            }
        }

        static DataContractJsonSerializer BuildSerializer(Type objectType)
        {
            var settings = new DataContractJsonSerializerSettings
            {
                UseSimpleDictionaryFormat = true,

            };
            return new DataContractJsonSerializer(objectType, settings);
        }
    }
}