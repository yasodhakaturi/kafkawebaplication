// Copyright 2020 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Refer to LICENSE for more information.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Web.Operations;

namespace Web
{
    /// <summary>
    ///     A simple example demonstrating how to set up a Kafka consumer as an
    ///     IHostedService.
    /// </summary>
    public class INDconsumer : BackgroundService
    {
        private readonly string RequestTimeTopic;
        private readonly string FrivolousTopic;
        private readonly IConsumer<Null, string> RequestTimekafkaConsumer;
        private readonly IConsumer<string, string> FrivolousTopickafkaConsumer;
        int count = 0;
        public INDconsumer(IConfiguration config)
        {

           // var consumerConfig = new ConsumerConfig();
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = Helper.BootstrapServers,
                GroupId = "csharp-consumer",
                EnableAutoCommit = false,
                StatisticsIntervalMs = 5000,
                SessionTimeoutMs = 6000,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnablePartitionEof = true,
                //MaxPollIntervalMs=-1,
                // A good introduction to the CooperativeSticky assignor and incremental rebalancing:
                // https://www.confluent.io/blog/cooperative-rebalancing-in-kafka-streams-consumer-ksqldb/
                PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky
            };
            config.GetSection("Kafka:ConsumerSettings").Bind(consumerConfig);
            this.RequestTimeTopic = config.GetValue<string>("Kafka:RequestTimeTopic");
            this.FrivolousTopic= config.GetValue<string>("Kafka:FrivolousTopic");
            this.RequestTimekafkaConsumer = new ConsumerBuilder<Null, string>(consumerConfig).Build();
            this.FrivolousTopickafkaConsumer= new ConsumerBuilder<string, string>(consumerConfig).Build();
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            //List<string> topics = new List<string> { "RequestTimeTopic", "FrivolousTopic" };
            //Run_Consume(Helper.BootstrapServers,topics,stoppingToken);
            new Thread(() => StartConsumerLoop(stoppingToken)).Start();
            return Task.CompletedTask;
        }

        private void StartConsumerLoop(CancellationToken cancellationToken)
        {
            RequestTimekafkaConsumer.Subscribe(this.RequestTimeTopic);
            FrivolousTopickafkaConsumer.Subscribe(this.FrivolousTopic);
            while (!cancellationToken.IsCancellationRequested)
            {
                if (count < 10)
                {
                    try
                    {
                        if (count < 4)
                        {
                            //var cr = this.RequestTimekafkaConsumer.Consume(cancellationToken);
                            var cr = this.RequestTimekafkaConsumer.Consume(10);
                            if (cr != null&&cr.Message!=null)
                                Console.WriteLine($"INDconsumer -- Received message at {cr.TopicPartitionOffset}: {cr.Message.Value}");
                            if (cr == null)
                            {
                                var cr1 = this.FrivolousTopickafkaConsumer.Consume(cancellationToken);
                                //Console.WriteLine($"INDconsumer--FrivolousTopickafkaConsumer -- Message: {cr.Message.Value} received from {cr.TopicPartitionOffset} at {cr.Timestamp.UtcDateTime} ");
                                if (cr1 != null && cr.Message != null)
                                    Console.WriteLine($"INDconsumer -- Received message at {cr1.TopicPartitionOffset}: {cr1.Message.Value}");

                            }

                            count++;
                            //if (cr != null && cr.IsPartitionEOF)
                            //{
                            //    Console.WriteLine(
                            //        $"Reached end of topic {cr.Topic}, partition {cr.Partition}, offset {cr.Offset}.");

                            //    continue;
                        }    //}

                        else
                        {
                            //    TimeSpan ts = new TimeSpan(0, 0, 5);
                            //    Thread.Sleep(ts);
                            var cr = this.FrivolousTopickafkaConsumer.Consume(cancellationToken);
                            //Console.WriteLine($"INDconsumer--FrivolousTopickafkaConsumer -- Message: {cr.Message.Value} received from {cr.TopicPartitionOffset} at {cr.Timestamp.UtcDateTime} ");
                            if(cr!=null && cr.Message!=null)
                            Console.WriteLine($"INDconsumer -- Received message at {cr.TopicPartitionOffset}: {cr.Message.Value}");
                            count++;
                        }

                    }

                    catch (OperationCanceledException)
                    {

                    }
                    catch (ConsumeException e)
                    {
                        // Consumer errors should generally be ignored (or logged) unless fatal.
                        Console.WriteLine($"INDconsumer Consume error: {e.Error.Reason}");

                        if (e.Error.IsFatal)
                        {
                            // https://github.com/edenhill/librdkafka/blob/master/INTRODUCTION.md#fatal-consumer-errors
                            // break;
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine($"Unexpected error: {e}");
                        //break;
                    }
                }
                else
                    count = 0;
                
            }
        }

        public static void Run_Consume(string brokerList, List<string> topics, CancellationToken cancellationToken)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = brokerList,
                GroupId = "csharp-consumer",
                EnableAutoCommit = false,
                StatisticsIntervalMs = 5000,
                SessionTimeoutMs = 6000,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnablePartitionEof = true,
                // A good introduction to the CooperativeSticky assignor and incremental rebalancing:
                // https://www.confluent.io/blog/cooperative-rebalancing-in-kafka-streams-consumer-ksqldb/
                PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky
            };

            const int commitPeriod = 5;

            // Note: If a key or value deserializer is not set (as is the case below), the 
            // deserializer corresponding to the appropriate type from Confluent.Kafka.Deserializers
            // will be used automatically (where available). The default deserializer for string
            // is UTF8. The default deserializer for Ignore returns null for all input data
            // (including non-null data).
            using (var consumer = new ConsumerBuilder<Ignore, string>(config)
                // Note: All handlers are called on the main .Consume thread.
                .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
                .SetStatisticsHandler((_, json) => Console.WriteLine($"Statistics: {json}"))
                .SetPartitionsAssignedHandler((c, partitions) =>
                {
                    // Since a cooperative assignor (CooperativeSticky) has been configured, the
                    // partition assignment is incremental (adds partitions to any existing assignment).
                    Console.WriteLine($"Incremental partition assignment: [{string.Join(", ", partitions)}]");

                    // Possibly manually specify start offsets by returning a list of topic/partition/offsets
                    // to assign to, e.g.:
                    // return partitions.Select(tp => new TopicPartitionOffset(tp, externalOffsets[tp]));
                })
                .SetPartitionsRevokedHandler((c, partitions) =>
                {
                    // Since a cooperative assignor (CooperativeSticky) has been configured, the revoked
                    // assignment is incremental (may remove only some partitions of the current assignment).
                    Console.WriteLine($"Incremental partition revokation: [{string.Join(", ", partitions)}]");
                })
                .SetPartitionsLostHandler((c, partitions) =>
                {
                    // The lost partitions handler is called when the consumer detects that it has lost ownership
                    // of its assignment (fallen out of the group).
                    Console.WriteLine($"Partitions were lost: [{string.Join(", ", partitions)}]");
                })
                .Build())
            {
                consumer.Subscribe(topics);

                try
                {
                    while (true)
                    {
                        try
                        {
                            var consumeResult = consumer.Consume(cancellationToken);

                            if (consumeResult.IsPartitionEOF)
                            {
                                Console.WriteLine(
                                    $"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");

                                continue;
                            }

                            Console.WriteLine($"Received message at {consumeResult.TopicPartitionOffset}: {consumeResult.Message.Value}");

                            if (consumeResult.Offset % commitPeriod == 0)
                            {
                                // The Commit method sends a "commit offsets" request to the Kafka
                                // cluster and synchronously waits for the response. This is very
                                // slow compared to the rate at which the consumer is capable of
                                // consuming messages. A high performance application will typically
                                // commit offsets relatively infrequently and be designed handle
                                // duplicate messages in the event of failure.
                                try
                                {
                                    consumer.Commit(consumeResult);
                                }
                                catch (KafkaException e)
                                {
                                    Console.WriteLine($"Commit error: {e.Error.Reason}");
                                }
                            }
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Consume error: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine("Closing consumer.");
                    consumer.Close();
                }
            }
        }
        private void StartConsumerLoop1(CancellationToken cancellationToken)
        {
            RequestTimekafkaConsumer.Subscribe(this.RequestTimeTopic);

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var cr = this.RequestTimekafkaConsumer.Consume(cancellationToken);

                    // Handle message...
                    Console.WriteLine("\n UAEconsumer -- message received :Key=" + $"{cr.Message.Key}: {cr.Message.Value}");
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (ConsumeException e)
                {
                    // Consumer errors should generally be ignored (or logged) unless fatal.
                    Console.WriteLine($"UAEconsumer Consume error: {e.Error.Reason}");

                    if (e.Error.IsFatal)
                    {
                        // https://github.com/edenhill/librdkafka/blob/master/INTRODUCTION.md#fatal-consumer-errors
                        break;
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Unexpected error: {e}");
                    break;
                }
            }
        }

        public override void Dispose()
        {
            this.RequestTimekafkaConsumer.Close(); // Commit offsets and leave the group cleanly.
            this.RequestTimekafkaConsumer.Dispose();

            base.Dispose();
        }
    }
}
