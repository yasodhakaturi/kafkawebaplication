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
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;

namespace Web
{
    /// <summary>
    ///     A simple example demonstrating how to set up a Kafka consumer as an
    ///     IHostedService.
    /// </summary>
    public class AUSconsumer : BackgroundService
    {
        private readonly string topic;
        private readonly IConsumer<Null, string> kafkaConsumer;
        int count = 0;
        public AUSconsumer(IConfiguration config)
        {

            //var consumerConfig = new ConsumerConfig();
            var consumerConfig = new ConsumerConfig 
            {
                GroupId = "consumer-group",
                MaxInFlight = 10,
                PartitionAssignmentStrategy = PartitionAssignmentStrategy.RoundRobin };
            config.GetSection("Kafka:ConsumerSettings").Bind(consumerConfig);
            this.topic = config.GetValue<string>("Kafka:RequestTimeTopic");
            this.kafkaConsumer = new ConsumerBuilder<Null, string>(consumerConfig).Build();

        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            new Thread(() => StartConsumerLoop(stoppingToken)).Start();
            return Task.CompletedTask;
        }

        private void StartConsumerLoop(CancellationToken cancellationToken)
        {
            kafkaConsumer.Subscribe(this.topic);

            while (!cancellationToken.IsCancellationRequested)
            {
                //if (count < 4)
                //{
                try
                {
                    //this.kafkaConsumer.Assign(this.topic, 0, new Offset(lastConsumedOffset));
                    var cr = this.kafkaConsumer.Consume(cancellationToken);
                    count++;
                    //string crstr = cr.Headers.ToString() + "/ " + cr.Topic + " / " + cr.Key.ToString() + "/" + cr.Message + "/ " + cr.IsPartitionEOF + "/" + cr.Offset + "/ " + cr.Partition + "/" + cr.Timestamp.ToString() + "/" + cr.TopicPartition + "/" + cr.TopicPartitionOffset + "/" + cr.Value;
                    // Handle message...
                    //Console.WriteLine("\n UAEconsumer -- consumer string : " + $"{ crstr.ToString()}");
                    //Console.WriteLine("\n UAEconsumer -- message received :Key=" + $"{cr.Timestamp.UtcDateTime} : {cr.Message.Key}: {cr.Message.Value}");
                    //Console.WriteLine($"UAEconsumer -- Message: {cr.Message.Value} received from Topic : {cr.Topic} ,at partition {cr.Partition} , partitionoffset: {cr.TopicPartitionOffset} at { cr.Timestamp.UtcDateTime} ");
                    Console.WriteLine($"AUSconsumer -- Received message at {cr.TopicPartitionOffset}: {cr.Message.Value}");
                }
                catch (OperationCanceledException)
                {

                }
                catch (ConsumeException e)
                {
                    // Consumer errors should generally be ignored (or logged) unless fatal.
                    Console.WriteLine($"AUSconsumer Consume error: {e.Error.Reason}");

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
                //}
                //else
                //{
                //    TimeSpan ts = new TimeSpan(0, 0, 10);
                //    Thread.Sleep(ts);
                //    count = 0;
                //}
            }
        }
        private void StartConsumerLoop1(CancellationToken cancellationToken)
        {
            kafkaConsumer.Subscribe(this.topic);

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var cr = this.kafkaConsumer.Consume(cancellationToken);

                    // Handle message...
                    Console.WriteLine("\n AUSconsumer -- message received :Key=" + $"{cr.Message.Key}: {cr.Message.Value}");
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (ConsumeException e)
                {
                    // Consumer errors should generally be ignored (or logged) unless fatal.
                    Console.WriteLine($"AUSconsumer Consume error: {e.Error.Reason}");

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
            this.kafkaConsumer.Close(); // Commit offsets and leave the group cleanly.
            this.kafkaConsumer.Dispose();

            base.Dispose();
        }
    }
}
