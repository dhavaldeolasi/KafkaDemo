using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaDemo
{
    public class KafkaProducerHostedService : IHostedService
    {
        private readonly ILogger<KafkaProducerHostedService> logger;
        private IProducer<Null, string> _producer;

        public KafkaProducerHostedService(ILogger<KafkaProducerHostedService> logger)
        {
            this.logger = logger;
            var config = new ProducerConfig()
            {
                BootstrapServers = "localhost:9092"
            };
            _producer = new ProducerBuilder<Null, string>(config).Build();
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            for (int i = 0; i < 10; i++)
            {
                var value = $"Hello World {i}";
                logger.LogInformation(value);
                await _producer.ProduceAsync("sample", new Message<Null, string>()
                {
                    Value = value
                }, cancellationToken);
            }
            _producer.Flush(TimeSpan.FromSeconds(10));

        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _producer?.Dispose();
            return Task.CompletedTask;
        }
    }
}
