namespace Consumer;
class Program
{
    private const string StreamKey = "mystream";
    private const string ConsumerGroup = "mygroup";
    private const string ConsumerName = "consumer2";
    private const int ProcessingDelay = 0;  // simulate a scenario where the consumer gets stuck
    
    static async Task RunConsumerAsync(CancellationToken token)
    {
        var consumer = new RedisStreamConsumer(StreamKey, ConsumerGroup, ConsumerName);
        try
        {
            // Initialize and register consumer
            await consumer.InitializeAsync();
            Console.WriteLine($"[Consumer 2] Listening for messages at {DateTime.Now}");
        
            // Start all consumer tasks
            var listenTask = consumer.StartListeningAsync(token, ProcessingDelay);
            var monitorTask = consumer.MonitorPendingEntriesAsync(token);
            var heartbeatTask = consumer.UpdateHeartbeatAsync(token);
        
            await Task.WhenAll(listenTask, monitorTask, heartbeatTask);
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("[Consumer 2] Operations cancelled gracefully.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Consumer 2] Error: {ex.Message}");
            throw;
        }
    }
    
    static async Task Main(string[] args)
    {
        var cts = new CancellationTokenSource();

        Console.CancelKeyPress += (sender, eventArgs) =>
        {
            Console.WriteLine("Ctrl+C pressed! Shutting down...");
            eventArgs.Cancel = true; 
            cts.Cancel();
        };
        
        try
        {
            await RunConsumerAsync(cts.Token);
        }
        catch (TaskCanceledException)
        {
            Console.WriteLine("[Main] Consumer task cancelled.");
        }
        Console.WriteLine("Shutdown Complete.");
    }
}