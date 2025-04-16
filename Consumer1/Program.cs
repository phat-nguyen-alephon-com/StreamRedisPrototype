using System.Text.Json;
using StackExchange.Redis;

namespace Consumer;
using BaseRedis;

public class RedisStreamConsumer: RedisConnectionBase
{
    private const int HeartbeatInterval = 10000; // milliseconds
    private const int ErrorRetryDelay = 1000; // milliseconds
    private const int PendingMessageBatchSize = 10; 
    public RedisStreamConsumer( string streamKey, string consumerGroup, string consumerName)
        : base( streamKey, consumerGroup, consumerName)
    {
    }

    public async Task InitializeAsync()
    {
        try
        {
            if (!(await _db.KeyExistsAsync(_streamKey)) ||
                (await _db.StreamGroupInfoAsync(_streamKey)).All(x => x.Name != _consumerGroup))
            {
                await _db.StreamCreateConsumerGroupAsync(_streamKey, _consumerGroup, "0-0", true);
            }

        }
        catch (RedisServerException ex) when (ex.Message.Contains("BUSYGROUP"))
        {
            
        }
    }
    private async Task ProcessPendingMessagesAsync(int processingDelay, CancellationToken cancellationToken)
    {
        var pendingMessages = await _db.StreamPendingMessagesAsync(
            _streamKey, _consumerGroup, count: PendingMessageBatchSize, consumerName: _consumerName);

        if (pendingMessages.Length > 0)
        {
            foreach (var pending in pendingMessages)
            {
                var entryId = pending.MessageId;
                await ProcessMessageByIdAsync(entryId, processingDelay, cancellationToken, isPending: true);
            }
        }
    }
    private async Task ProcessMessageByIdAsync(string entryId, int processingDelay, 
        CancellationToken cancellationToken, bool isPending = false)
    {
        var entries = await _db.StreamRangeAsync(_streamKey, entryId, entryId);
        
        if (entries != null && entries.Any())
        {
            var entry = entries.First();
            var dataEntry = entry.Values.FirstOrDefault(x => x.Name == "data");
            var messageJson = !dataEntry.Value.IsNull ? dataEntry.Value.ToString() : null;

            if (!string.IsNullOrWhiteSpace(messageJson))
            {
                var account = JsonSerializer.Deserialize<Account>(messageJson);
                string messageType = isPending ? "Pending/Reclaimed" : "New";
                
                Console.WriteLine(
                    $"ID: {entryId}, Processed {messageType} Account: ID={account.AccountId}, " +
                    $"Platform={account.Platform}, Balance={account.Balance}, Equity={account.Equity}");

                await _db.StreamAcknowledgeAsync(_streamKey, _consumerGroup, entryId);
            }
        }
    }
    private async Task ProcessNewMessagesAsync(int processingDelay, CancellationToken cancellationToken)
    {
        var result = await _db.ExecuteAsync(
            "XREADGROUP",
            "GROUP", _consumerGroup, _consumerName,
            "COUNT", "10",
            "BLOCK", 100000,
            "STREAMS", _streamKey, ">");

        if (result.IsNull) return;
        
        var streamResult = (RedisResult[])result;
        foreach (var stream in streamResult)
        {
            var streamArray = (RedisResult[])stream;
            var entries = (RedisResult[])streamArray[1];
            
            foreach (var entry in entries)
            {
                var entryArray = (RedisResult[])entry;
                var entryId = (string)entryArray[0];
                var values = (RedisResult[])entryArray[1];

                var jsonValue = values.Skip(1).FirstOrDefault()?.ToString();

                if (!string.IsNullOrWhiteSpace(jsonValue))
                {
                    var account = JsonSerializer.Deserialize<Account>(jsonValue);
                    Console.WriteLine(
                        $"ID: {entryId}, Processed Account: ID={account.AccountId}, " +
                        $"Platform={account.Platform}, Balance={account.Balance}, Equity={account.Equity}");
                }
                await Task.Delay(processingDelay, cancellationToken);
                await _db.StreamAcknowledgeAsync(_streamKey, _consumerGroup, entryId);
            }
        }
    }
    


    public async Task UpdateHeartbeatAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            await _db.SortedSetAddAsync(_consumerGroup, _consumerName, timestamp);
            await Task.Delay(HeartbeatInterval, cancellationToken);
        }
    }
    public async Task StartListeningAsync(CancellationToken cancellationToken, int processingDelay)
    {            
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                // Process any pending messages first
                await ProcessPendingMessagesAsync(processingDelay, cancellationToken);
                
                // Then read new messages
                await ProcessNewMessagesAsync(processingDelay, cancellationToken);
            }        
            catch (Exception ex)
            {
                Console.WriteLine($"Error processing stream: {ex.Message}");
                await Task.Delay(ErrorRetryDelay, cancellationToken);
            }
        }
    }
}

class Program
{
    private const string StreamKey = "mystream";
    private const string ConsumerGroup = "mygroup";
    private const string ConsumerName = "consumer1";
    private const int ProcessingDelay = 0; // milliseconds
    
    static async Task RunConsumerAsync(CancellationToken token)
    {
        var consumer = new RedisStreamConsumer(StreamKey, ConsumerGroup, ConsumerName);
    
        try
        {
            await consumer.InitializeAsync();
            Console.WriteLine($"[Consumer 1] Listening for messages at {DateTime.Now}");
            var listenTask = consumer.StartListeningAsync(token, ProcessingDelay);
            var monitorTask = consumer.MonitorPendingEntriesAsync(token);
            var heartbeatTask = consumer.UpdateHeartbeatAsync(token);
            await Task.WhenAll(listenTask, monitorTask, heartbeatTask);
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("[Consumer 1] Operations cancelled gracefully.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Consumer 1] Error: {ex.Message}");
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
            Console.WriteLine("[Main] Consumer 1 task cancelled.");
        }

        Console.WriteLine("Shutdown Complete.");
    }
}