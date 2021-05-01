using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
namespace Web.Operations
{
    public class Helper
    {

        public static string BootstrapServers
        {
            get
            {
                var config = new ConfigurationBuilder()
               .SetBasePath(AppDomain.CurrentDomain.BaseDirectory)
               .AddJsonFile("appsettings.Development.json").Build();


                //IConfiguration config;
                
                string bootstrapservers = config.GetSection("Kafka:ProducerSettings:BootstrapServers").Value;
                
                return bootstrapservers;
            }
            set { }
        }
    }
}
