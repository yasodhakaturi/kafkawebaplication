using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Web.Models;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Web.Operations;
using Partition = Web.Models.Partition;

namespace Web.Operations
{
    public class TopicManagerImpl:TopicManager
    {
       public void addTopic(String topicName, int partitionCount)
        {
           
            string bootstrapServers = Helper.BootstrapServers;
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                try
                {
                     adminClient.CreateTopicsAsync(new TopicSpecification[] {
                        new TopicSpecification { Name = topicName, ReplicationFactor = 1, NumPartitions = partitionCount} });
                }
                catch (CreateTopicsException e)
                {
                    Console.WriteLine($"An error occured creating topic {e.Results[0].Topic}: {e.Results[0].Error.Reason}");
                }
            }
        }

        public void IncreasePartitions(String topicName, int partitionCount)
        {

            string bootstrapServers = Helper.BootstrapServers;
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                try
                {
                    adminClient.CreatePartitionsAsync(new PartitionsSpecification[] {
                        new PartitionsSpecification { Topic = topicName, IncreaseTo = partitionCount} });
                }
                catch (CreateTopicsException e)
                {
                    Console.WriteLine($"An error occured creating topic {e.Results[0].Topic}: {e.Results[0].Error.Reason}");
                }
            }
        }

        public Topic gettopic(string topicname,string bootstrapServers)
        {
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                var meta = adminClient.GetMetadata(topicname ,TimeSpan.FromSeconds(20));
                Topic ts = new Topic();
                ts.partitionCount = meta.Topics[0].Partitions.Count;
                ts.topicName = meta.Topics[0].Topic;
               
                meta.Topics[0].Partitions.ForEach(partition =>
                {
                    Partition pr = new Partition();
                    pr.PartitionId = partition.PartitionId;
                    pr.Leader = partition.Leader;
                    pr.Replicas = partition.Replicas;
                    pr.InSyncReplicas = partition.InSyncReplicas;
                    ts.partitions.Add(pr);
                });
                //ts.topicName=meta.Topics.Find(topicname)
                return ts;
            }
            }
        public  KafkaServerDetails GetkafkaserverDetails(string bootstrapServers)
        {
            KafkaServerDetails kafkadetails = new KafkaServerDetails();
            
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {

                //groupinfo
                var groups = adminClient.ListGroups(TimeSpan.FromSeconds(10));
                foreach (var g in groups)
                {
                    Groupinfo grinfo = new Groupinfo();
                    grinfo.Group = g.Group;
                    Broker br = new Broker();
                    br.BrokerId = g.Broker.BrokerId;
                    br.Host = g.Broker.Host;
                    br.Port = g.Broker.Port;
                    grinfo.broker = br;
                    grinfo.State = g.State;
                    grinfo.Protocol = g.Protocol;
                    grinfo.ProtocolType = g.ProtocolType;
                    if (g.Members.Count == 0)
                    {
                        g.Members.ForEach(member =>
                        {
                            GroupMember grmem = new GroupMember();
                            grmem.MemberId = member.MemberId;
                            grmem.ClientId = member.ClientId;
                            grmem.ClientHost = member.ClientHost;
                            grmem.MemberAssignment = member.MemberAssignment;
                            grmem.MemberMetadata = member.MemberMetadata;
                            grinfo.Members.Add(grmem);
                        });
                    }
                    kafkadetails.groupinfo.Add(grinfo);
                }


                    // Warning: The API for this functionality is subject to change.
                    var meta = adminClient.GetMetadata(TimeSpan.FromSeconds(20));
                kafkadetails.OriginatingBrokerId = meta.OriginatingBrokerId;
                kafkadetails.OriginatingBrokerName = meta.OriginatingBrokerName;
                //Console.WriteLine($"{meta.OriginatingBrokerId} {meta.OriginatingBrokerName}");
                //foreach (BrokerMetadata b in meta.Brokers)
                //{
                //    Broker br = new Broker();
                //    br.BrokerId = b.BrokerId;
                //    br.Host = b.Host;
                //    br.Port = b.Port;
                //    kafkadetails.brokers.Add(br);
                //}
                meta.Brokers.ForEach(broker =>
                {
                    Broker br = new Broker();
                    br.BrokerId = broker.BrokerId;
                    br.Host = broker.Host;
                    br.Port = broker.Port;
                    kafkadetails.brokers.Add(br);
                });
                    //Console.WriteLine($"Broker: {broker.BrokerId} {broker.Host}:{broker.Port}") ) ;

                meta.Topics.ForEach(topic =>
                {
                    Topic t = new Topic();
                    //Console.WriteLine($"Topic: {topic.Topic} {topic.Error}");
                    t.topicName = topic.Topic;
                    t.partitionCount = topic.Partitions.Count;
                    topic.Partitions.ForEach(partition =>
                    {
                        Partition p = new Partition();
                        p.PartitionId = partition.PartitionId;
                        p.Replicas =  partition.Replicas;
                        p.InSyncReplicas = partition.InSyncReplicas;
                        t.partitions.Add(p);
                        //Console.WriteLine($"  Partition: {partition.PartitionId}");
                        //Console.WriteLine($"    Replicas: {partition.Replicas.ToString()}");
                        //Console.WriteLine($"    InSyncReplicas: {partition.InSyncReplicas.ToString()}");
                    });
                    kafkadetails.topics.Add(t);
                });
            }
            return kafkadetails;
        }

        public List<string> gettopics()
        {
            List<string> topicnames = new List<string>();
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = Helper.BootstrapServers }).Build())
            {
                var meta = adminClient.GetMetadata(TimeSpan.FromSeconds(20));

                meta.Topics.ForEach(topic =>
                { topicnames.Add(topic.Topic); });
            }
            return topicnames;
        }

       public  void deleteTopics(string brokerList, IEnumerable<string> topicNameList)
        {
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = brokerList }).Build())
            {
                adminClient.DeleteTopicsAsync(topicNameList, null);
            }
        }
    }
}
