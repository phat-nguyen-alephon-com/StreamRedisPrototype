using System.Text.Json;
using BaseRedis;
using StackExchange.Redis;

namespace Red;
class Program
{
    private const string StreamKey = "mystream";
    private const string ConsumerGroup = "mygroup";
    private const int MessageDelay = 100;

    static async Task Main(string[] args)
    {
        var cts = new CancellationTokenSource();
        SetupShutdownHandlers(cts);
        
        try
        {
            var dx =  RunProducerAsync(cts.Token, "DX");
            var tl =  RunProducerAsync(cts.Token, "TL");
            await Task.WhenAll(dx, tl);
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("[Main] Producer task cancelled gracefully.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Main] Unexpected error: {ex.Message}");
        }
        finally
        {
            Console.WriteLine("Shutdown Complete.");
        }
    }

    private static void SetupShutdownHandlers(CancellationTokenSource cts)
    {
        Console.CancelKeyPress += (sender, eventArgs) =>
        {
            Console.WriteLine("Ctrl+C pressed! Shutting down...");
            eventArgs.Cancel = true; // Prevent abrupt termination
            cts.Cancel();
        };
    }

    private static async Task RunProducerAsync(CancellationToken token, string platform)
    {
        var producer = new RedisStreamProducer(StreamKey);
        
        try
        {
            while (!token.IsCancellationRequested)
            {
                Console.WriteLine($"[Producer {platform}] Sending account update at {DateTime.Now}");
                var account = AccountGenerator.GenerateRandomAccount(platform);
                await producer.AddMessageAsync(account);
                await Task.Delay(MessageDelay, token); 
            }
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("[Producer {platform}] Producer operation cancelled gracefully.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Producer {platform}] Error in producer: {ex.Message}");
            throw; 
        }
        finally
        {
            Console.WriteLine($"[Producer {platform}] Stopped.");
        }
    }
}

public class RedisStreamProducer : RedisConnectionBase
{
    public RedisStreamProducer(string streamKey)
        : base(streamKey, null, null)
    {
    }
    
    public async Task AddMessageAsync(Account account)
    {
        var json = JsonSerializer.Serialize(account);
        var entry = new NameValueEntry("data", json);
        await _db.StreamAddAsync(_streamKey, new[] { entry });
    }
}