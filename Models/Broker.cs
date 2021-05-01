using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Web.Models
{
    public class Broker
    {
        //
        // Summary:
        //     Gets the Kafka broker id.
        public int BrokerId { get; set; }
        //
        // Summary:
        //     Gets the Kafka broker hostname.
        public string Host { get; set; }
        //
        // Summary:
        //     Gets the Kafka broker port.
        public int Port { get; set; }

    }
}
