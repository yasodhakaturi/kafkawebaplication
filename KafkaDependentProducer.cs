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

using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System;
using System.Linq;
using System.Net.Http;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;


namespace Web
{
    /// <summary>
    ///     Leverages the injected KafkaClientHandle instance to allow
    ///     Confluent.Kafka.Message{K,V}s to be produced to Kafka.
    /// </summary>
    public class KafkaDependentProducer<K,V>
    {
        IProducer<K, V> kafkaHandle;

        public KafkaDependentProducer(KafkaClientHandle handle)
        {
            kafkaHandle = new DependentProducerBuilder<K, V>(handle.Handle).Build();
        }

        /// <summary>
        ///     Asychronously produce a message and expose delivery information
        ///     via the returned Task. Use this method of producing if you would
        ///     like to await the result before flow of execution continues.
        /// <summary>
        public Task ProduceAsync(string topic, Message<K, V> message)
            => this.kafkaHandle.ProduceAsync(topic, message);

        /// <summary>
        ///     Asynchronously produce a message and expose delivery information
        ///     via the provided callback function. Use this method of producing
        ///     if you would like flow of execution to continue immediately, and
        ///     handle delivery information out-of-band.
        /// </summary>
        /// 

        public void Produce1(TopicPartition tp, Message<K, V> message, Action<DeliveryReport<K, V>> deliveryHandler = null)
       => this.kafkaHandle.Produce(tp, message, deliveryHandler);
        public void Produce(string topic, Message<K, V> message, Action<DeliveryReport<K, V>> deliveryHandler = null)
            => this.kafkaHandle.Produce(topic, message, deliveryHandler);

         /// <summary>
        ///     Generate example line input data (using the source code of this example as the source!).
        /// </summary>
       public  async Task Generator_LineInputData(string brokerList, string clientId, CancellationToken ct)
        {
            const string Topic_InputLines = "RequestTimeTopic";
            var client = new HttpClient();
            var r = await client.GetAsync("https://raw.githubusercontent.com/confluentinc/confluent-kafka-dotnet/master/examples/ExactlyOnce/Program.cs", ct);
            r.EnsureSuccessStatusCode();
            var content = await r.Content.ReadAsStringAsync();
            var lines = content.Split('\n');

            var pConfig = new ProducerConfig
            {
                BootstrapServers = brokerList,
                ClientId = clientId
                
                 
            };

           // Console.WriteLine($"Producing text line data to topic: {Topic_InputLines}");
            using (var producer = new ProducerBuilder<Null, string>(pConfig).Build())
            {
                var lCount = 0;
                foreach (var l in lines)
                {
                    await Task.Delay(TimeSpan.FromSeconds(1), ct);  // slow down the calls to produce to make the output more interesting to watch.
                   var deliveryReport= await producer.ProduceAsync(Topic_InputLines, new Message<Null, string> { Value = l }, ct);  // Note: producing synchronously is slow and should generally be avoided.
                    lCount += 1;
                    Console.WriteLine($"Producer-deliveryReportHandler---Message delivered on Topic: {deliveryReport.Topic} ,at partition : {deliveryReport.TopicPartition} ,Offset:{deliveryReport.TopicPartitionOffset}");

                    //if (lCount % 10 == 0)
                    //{
                    //    Console.WriteLine($"Produced {lCount} input lines.");
                    //}
                }

                producer.Flush(ct);
            }

            Console.WriteLine("Generator_LineInputData: Wrote all input lines to Kafka");
        }


        /// <summary>
        ///     A transactional (exactly once) processing loop that reads lines of text from
        ///     Topic_InputLines, splits them into words, and outputs the result to Topic_Words.
        /// </summary>
        static void Processor_MapWords(string brokerList, string clientId, CancellationToken ct)
        {
            const string TransactionalIdPrefix_MapWords = "map-words-transaction-id";
            const string ConsumerGroup_MapWords = "map-words-consumer-group";
             TimeSpan DefaultTimeout = TimeSpan.FromSeconds(30);
            const string Topic_InputLines = "lines";
            const string Topic_Words = "words";

            if (clientId == null)
            {
                throw new Exception("Map processor requires that a client id is specified.");
            }

            var pConfig = new ProducerConfig
            {
                BootstrapServers = brokerList,
                ClientId = clientId + "_producer",
                // The TransactionalId identifies this instance of the map words processor.
                // If you start another instance with the same transactional id, the existing
                // instance will be fenced.
                TransactionalId = TransactionalIdPrefix_MapWords + "-" + clientId
            };

            var cConfig = new ConsumerConfig
            {
                BootstrapServers = brokerList,
                ClientId = clientId + "_consumer",
                GroupId = ConsumerGroup_MapWords,
                // AutoOffsetReset specifies the action to take when there
                // are no committed offsets for a partition, or an error
                // occurs retrieving offsets. If there are committed offsets,
                // it has no effect.
                AutoOffsetReset = AutoOffsetReset.Earliest,
                // Offsets are committed using the producer as part of the
                // transaction - not the consumer. When using transactions,
                // you must turn off auto commit on the consumer, which is
                // enabled by default!
                EnableAutoCommit = false,
                // Enable incremental rebalancing by using the CooperativeSticky
                // assignor (avoid stop-the-world rebalances).
                PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky
            };

            var txnCommitPeriod = TimeSpan.FromSeconds(10);

            var lastTxnCommit = DateTime.Now;

            using (var producer = new ProducerBuilder<string, Null>(pConfig).Build())
            using (var consumer = new ConsumerBuilder<Null, string>(cConfig)
                .SetPartitionsRevokedHandler((c, partitions) => {
                    var remaining = c.Assignment.Where(tp => partitions.Where(x => x.TopicPartition == tp).Count() == 0);
                    Console.WriteLine(
                        "** MapWords consumer group partitions revoked: [" +
                        string.Join(',', partitions.Select(p => p.Partition.Value)) +
                        "], remaining: [" +
                        string.Join(',', remaining.Select(p => p.Partition.Value)) +
                        "]");

                    // All handlers (except the log handler) are executed as a
                    // side-effect of, and on the same thread as the Consume or
                    // Close methods. Any exception thrown in a handler (with
                    // the exception of the log and error handlers) will
                    // be propagated to the application via the initiating
                    // call. i.e. in this example, any exceptions thrown in this
                    // handler will be exposed via the Consume method in the main
                    // consume loop and handled by the try/catch block there.

                    producer.SendOffsetsToTransaction(
                        c.Assignment.Select(a => new TopicPartitionOffset(a, c.Position(a))),
                        c.ConsumerGroupMetadata,
                        DefaultTimeout);
                    producer.CommitTransaction();
                    producer.BeginTransaction();
                })

                .SetPartitionsLostHandler((c, partitions) => {
                    // Ownership of the partitions has been involuntarily lost and
                    // are now likely already owned by another consumer.

                    Console.WriteLine(
                        "** MapWords consumer group partitions lost: [" +
                        string.Join(',', partitions.Select(p => p.Partition.Value)) +
                        "]");

                    producer.AbortTransaction();
                    producer.BeginTransaction();
                })

                .SetPartitionsAssignedHandler((c, partitions) => {
                    Console.WriteLine(
                        "** MapWords consumer group additional partitions assigned: [" +
                        string.Join(',', partitions.Select(p => p.Partition.Value)) +
                        "], all: [" +
                        string.Join(',', c.Assignment.Concat(partitions).Select(p => p.Partition.Value)) +
                        "]");

                    // No action is required here related to transactions - offsets
                    // for the newly assigned partitions will be committed in the
                    // main consume loop along with those for already assigned
                    // partitions as per usual.
                })
                .Build())
            {
                consumer.Subscribe(Topic_InputLines);

                producer.InitTransactions(DefaultTimeout);
                producer.BeginTransaction();

                var wCount = 0;
                var lCount = 0;
                while (true)
                {
                    try
                    {
                        ct.ThrowIfCancellationRequested();

                        // Do not block on Consume indefinitely to avoid the possibility of a transaction timeout.
                        var cr = consumer.Consume(TimeSpan.FromSeconds(1));

                        if (cr != null)
                        {
                            lCount += 1;

                            var words = Regex.Split(cr.Message.Value.ToLower(), @"[^a-zA-Z_]").Where(s => s != String.Empty);
                            foreach (var w in words)
                            {
                                while (true)
                                {
                                    try
                                    {
                                        producer.Produce(Topic_Words, new Message<string, Null> { Key = w });
                                        // Note: when using transactions, there is no need to check for errors of individual
                                        // produce call delivery reports because if the transaction commits successfully, you
                                        // can be sure that all the constituent messages were delivered successfully and in order.

                                        wCount += 1;
                                    }
                                    catch (KafkaException e)
                                    {
                                        // An immediate failure of the produce call is most often caused by the
                                        // local message queue being full, and appropriate response to that is
                                        // to wait a bit and retry.
                                        if (e.Error.Code == ErrorCode.Local_QueueFull)
                                        {
                                            Thread.Sleep(TimeSpan.FromSeconds(1000));
                                            continue;
                                        }
                                        throw;
                                    }
                                    break;
                                }
                            }
                        }

                        // Commit transactions every TxnCommitPeriod
                        if (DateTime.Now > lastTxnCommit + txnCommitPeriod)
                        {
                            // Note: Exceptions thrown by SendOffsetsToTransaction and
                            // CommitTransaction that are not marked as fatal can be
                            // recovered from. However, in order to keep this example
                            // short(er), the additional logic required to achieve this
                            // has been omitted. This should happen only rarely, so
                            // requiring a process restart in this case is not necessarily
                            // a bad compromise, even in production scenarios.

                            producer.SendOffsetsToTransaction(
                                // Note: committed offsets reflect the next message to consume, not last
                                // message consumed. consumer.Position returns the last consumed offset
                                // values + 1, as required.
                                consumer.Assignment.Select(a => new TopicPartitionOffset(a, consumer.Position(a))),
                                consumer.ConsumerGroupMetadata,
                                DefaultTimeout);
                            producer.CommitTransaction();
                            producer.BeginTransaction();

                            Console.WriteLine($"Committed MapWords transaction(s) comprising {wCount} words from {lCount} lines.");
                            lastTxnCommit = DateTime.Now;
                            wCount = 0;
                            lCount = 0;
                        }
                    }
                    catch (Exception e)
                    {
                        // Attempt to abort the transaction (but ignore any errors) as a measure
                        // against stalling consumption of Topic_Words.
                        producer.AbortTransaction();

                        Console.WriteLine("Exiting MapWords consume loop due to an exception: " + e);
                        // Note: transactions may be committed / aborted in the partitions
                        // revoked / lost handler as a side effect of the call to close.
                        consumer.Close();
                        Console.WriteLine("MapWords consumer closed");
                        break;
                    }

                    // Assume the presence of an external system that monitors whether worker
                    // processes have died, and restarts new instances as required. This
                    // setup is typical, and avoids complex error handling logic in the
                    // client code.
                }
            }
        }
        private void deliveryReportHandler(DeliveryReport<Null, string> deliveryReport)
        {
            ILogger logger;
            if (deliveryReport.Status == PersistenceStatus.NotPersisted)
            {
                // It is common to write application logs to Kafka (note: this project does not provide
                // an example logger implementation that does this). Such an implementation should
                // ideally fall back to logging messages locally in the case of delivery problems.
                //logger.Log(LogLevel.Warning, $"Message delivery failed: {deliveryReport.Message.Value}");
                Console.WriteLine($"Message delivery failed: {deliveryReport.Message.Value}");
            }
            else
            {
                //logger.Log(LogLevel.Information, $"INDdeliveryReportHandler---Message delivered on Topic: {deliveryReport.Topic} ,at partition : {deliveryReport.TopicPartition} ,Offset:{deliveryReport.TopicPartitionOffset}");
                Console.WriteLine($"deliveryReportHandler---Message delivered on Topic: {deliveryReport.Topic} ,at partition : {deliveryReport.TopicPartition} ,Offset:{deliveryReport.TopicPartitionOffset}");
            }
        }
        public void Flush(TimeSpan timeout)
            => this.kafkaHandle.Flush(timeout);
    }
}
