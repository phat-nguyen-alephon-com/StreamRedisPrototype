using StackExchange.Redis;
using Redis.OM;
using Redis.OM.Modeling;
namespace BaseRedis;

// using StackExchange.Redis;

public class RedisConnectionBase
{
    public readonly ConnectionMultiplexer _redis;
    public readonly IDatabase _db;
    protected readonly string _streamKey;
    protected readonly string _consumerGroup;
    protected readonly string _consumerName;
    private readonly string host = "redis-10701.c114.us-east-1-4.ec2.redns.redis-cloud.com";
    private readonly int port = 10701;
    private readonly string user = "default";
    private readonly string password = "eh3Td23NsIP5CQIkiHViFOhH9piG9OOk";
    protected RedisConnectionProvider _provider;

    public RedisConnectionBase( string streamKey, string consumerGroup, string consumerName)
    {
        var config = new ConfigurationOptions
        {
            EndPoints = { { host, port } },
            User = user,
            Password = password,
            AbortOnConnectFail = false,
            AsyncTimeout = 60000,
            SyncTimeout = 60000,
            ConnectTimeout = 60000,
        };

        _redis = ConnectionMultiplexer.Connect(config);
        _db = _redis.GetDatabase();
        _provider = new RedisConnectionProvider(_redis);

        _streamKey = streamKey;
        _consumerGroup = consumerGroup;
        _consumerName = consumerName;
    }
    public async Task MonitorPendingEntriesAsync(CancellationToken cancellationToken)
    {
        var nextClaimId = "0-0";
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var activeConsumers = await _db.SortedSetRangeByRankAsync(_consumerGroup, 0, 0, Order.Descending);
                if (activeConsumers.Length ==0)
                {
                    Console.WriteLine("No other active consumers available for redistribution.");
                    await Task.Delay(10000, cancellationToken);
                    continue;
                }

                var targetConsumer = activeConsumers[0];
               
                var reclaimed = await _db.StreamAutoClaimAsync(
                    _streamKey, 
                    _consumerGroup, 
                    targetConsumer, 
                    30000,    // Idle > 60 seconds 
                    nextClaimId,         // Start from beginning
                    10);              // Limit to 10 messages

                if (!reclaimed.IsNull && reclaimed.ClaimedEntries.Any())
                {
                    foreach (var message in reclaimed.ClaimedEntries)
                    {
                        Console.WriteLine($"Reassigned message {message.Id} to {targetConsumer}");
                    }
                }
                nextClaimId = reclaimed.NextStartId;

                await Task.Delay(10000, cancellationToken); // Check every 10 seconds
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error in auto-claim monitoring: {ex.Message}");
                await Task.Delay(5000, cancellationToken);
            }
        }
    }
}

[Document(StorageType = StorageType.Json, Prefixes = new[] {"account"})]
public class Account
{
    [RedisIdField]
    public string AccountId { get; set; } = string.Empty;
    public decimal Balance { get; set; } = decimal.Zero;
    public decimal Equity { get; set; } = decimal.Zero;
    public string Platform { get; set; } = string.Empty;
}
public static class AccountGenerator
{
    private static readonly Random _random = new Random();

    public static Account GenerateRandomAccount(string platform)
    {
        var balance = Math.Round((decimal)(_random.NextDouble() * 10000), 2);
        var equity = Math.Round(balance + (decimal)(_random.NextDouble() * 1000 - 500), 2);
            
        return new Account
        {
            AccountId = Guid.NewGuid().ToString(),
            Balance = balance,
            Equity = equity,
            Platform = platform
        };
    }

    public static Dictionary<string, string> ToDictionary(this Account account)
    {
        return new Dictionary<string, string>
        {
            { "AccountId", account.AccountId },
            { "Balance", account.Balance.ToString("F2") },
            { "Equity", account.Equity.ToString("F2") },
        };
    }
}