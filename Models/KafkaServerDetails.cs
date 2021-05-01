using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Web.Models
{
    public class KafkaServerDetails
    {
        public List<Broker> brokers = new List<Broker>();
        public List<Topic> topics = new List<Topic>();
        public List<Groupinfo> groupinfo = new List<Groupinfo>();
        public int OriginatingBrokerId { get; set; }
        //
        // Summary:
        //     Gets the name of the broker that provided this metadata.
        public string OriginatingBrokerName { get; set; }
        

    }
}
