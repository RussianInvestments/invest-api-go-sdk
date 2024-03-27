using System.Text;
using Tinkoff.InvestApi.V1;

namespace Tinkoff.InvestApi.Sample;

public class MarketDataServiceSample
{
    private readonly InvestApiClient _investApiClient;

    public MarketDataServiceSample(InvestApiClient investApiClient)
    {
        _investApiClient = investApiClient;
    }

    public async Task<string> GetTradingStatusesAsync(CancellationToken cancellationToken, string instrumentUid)
    {
        var request = new GetTradingStatusesRequest();
        request.InstrumentId.Add(instrumentUid);
        //или 
        //request.InstrumentId.AddRange(new List<string>() {instrumentUid});
        
        var tradingStatuses = await _investApiClient.MarketData.GetTradingStatusesAsync(request: request, cancellationToken:cancellationToken);

        return new TradingStatusesFormatter(tradingStatuses).Format();
    }

    public string GetTradingStatuses(string instrumentUid)
    {
        var request = new GetTradingStatusesRequest();
        request.InstrumentId.Add(instrumentUid);
        //или 
        //request.InstrumentId.AddRange(new List<string>() {instrumentUid});
        
        var tradingStatuses = _investApiClient.MarketData.GetTradingStatuses(request);

        return new TradingStatusesFormatter(tradingStatuses).Format();
    }
    
    public class TradingStatusesFormatter
    {
        private readonly GetTradingStatusesResponse _tradingStatusesResponse;

        public TradingStatusesFormatter(GetTradingStatusesResponse tradingStatusesResponse)
        {
            _tradingStatusesResponse = tradingStatusesResponse;
        }

        public string Format()
        {
            var builder = new StringBuilder();

            if (_tradingStatusesResponse.TradingStatuses.Any())
            {
                builder.AppendLine().AppendLine("TradingStatuses:");
                foreach (var status in _tradingStatusesResponse.TradingStatuses)
                {
                    builder.Append($"[{status}]");
                }
            }
            else
            {
                builder.Append("No trading statuses exists in response");
            }
            
            return builder.ToString();
        }
    }
    
    
}