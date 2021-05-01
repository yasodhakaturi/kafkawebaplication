using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Web.Models
{
    public class Partition
    {
        public int PartitionId;
        //
        // Summary:
        //     Gets the id of the broker that is the leader for the partition.
        public int Leader;
        //
        // Summary:
        //     Gets the ids of all brokers that contain replicas of the partition.
        public int[] Replicas;
        //
        // Summary:
        //     Gets the ids of all brokers that contain in-sync replicas of the partition.
        public int[] InSyncReplicas;
        //
        // Summary:
        //     Gets a rich Confluent.Kafka.PartitionMetadata.Error object associated with the
        //     request for this partition metadata. Note: this value is cached by the broker
        //     and is consequently not guaranteed to be up-to-date.
        //public Error Error;

    }
}
