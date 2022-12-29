using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaDemo
{
    public class KafkaConsumerHostedService : IHostedService
    {
        private readonly ILogger<KafkaConsumerHostedService> logger;
        private IConsumer<Null, string> _consumer;

        public KafkaConsumerHostedService(ILogger<KafkaConsumerHostedService> logger)
        {
            this.logger = logger;
            var config = new ConsumerConfig
            {
                GroupId = "sample-consumer-group",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };
            _consumer = new ConsumerBuilder<Null, string>(config).Build();
        }
        public Task StartAsync(CancellationToken cancellationToken)
        {
            _consumer.Subscribe("sample");

            try
            {
                while (true)
                {
                    var response = _consumer.Consume(cancellationToken);
                    if (response != null)
                    {
                        logger.LogInformation($"Received:{response.Message.Value}");
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _consumer?.Dispose();
            return Task.CompletedTask;
        }
    }
}
