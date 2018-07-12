/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

using NodaTime;
using System;
using System.Collections.Generic;
using QuantConnect.Data;
using QuantConnect.Packets;
using Newtonsoft.Json.Linq;
using System.Runtime.CompilerServices;
using QuantConnect.Interfaces;
using HistoryRequest = QuantConnect.Data.HistoryRequest;
using QuantConnect.Lean.Engine.DataFeeds.Queues;

namespace QuantConnect.ToolBox.Alphavantage
{
    /// <summary>
    /// Live Data Queue is the cut out implementation of how to bind a custom live data source 
    /// </summary>
    public class AlphavantageDataQueueHandler : LiveDataQueue, IHistoryProvider
    {
        private int _dataPointCount;
        
        public AlphavantageDataQueueHandler()
        {
            // Pseudo-code:
            // - Test the connection with Alphavantage, and throw an exception if it cannot be reached?
            // Get the Alphavantage API token from the config.json file. Store in member variable. Throw an error if not found. 
        }
        
        /// <summary>
        /// Gets the total number of data points emitted by this history provider
        /// </summary>
        public int DataPointCount
        {
            get { return _dataPointCount; }
        }
      
        /// <summary>
        /// Desktop/Local doesn't support live data from this handler
        /// </summary>
        /// <returns>Tick</returns>
        public sealed override IEnumerable<BaseData> GetNextTicks()
        {
            return null;
            // Pseudo-code
            /*
            - Iterate through the subscribed symbols
                - Query the data from Alphavantage
                - Add to the return: Each minute data that is later than the last tick received.
                    - If no tick has been received, return all ticks that are later than one resolution cycle ago.
                      Ex: If we requested hourly data for SPY symbol, return all data from the past hour. If not first tick received, return all data that is later than last tick received.
            */
        }

        /// <summary>
        /// Desktop/Local doesn't support live data from this handler
        /// </summary>
        public sealed override void Subscribe(LiveNodePacket job, IEnumerable<Symbol> symbols)
        {
            // Pseudo-code:
            // - Add the requested symbol and its resolution in a member variable that will contain the list of symbols to query
            // - This variable will be a dictionary that will contain:
            //     - {<Symbol_Name>: {"Resolution": <Requested resolution>, "Last tick time": <Timestamp of the last tick received for this symbol>}}
        }

        /// <summary>
        /// Desktop/Local doesn't support live data from this handler
        /// </summary>
        public sealed override void Unsubscribe(LiveNodePacket job, IEnumerable<Symbol> symbols)
        {
            // Pseudo-code:
            // - Remove the requested symbol from a member variable that will contain the list of symbols to query
        }

        /// <summary>
        /// Returns true if the given symbol is subscribed to.
        /// </summary>
        /// <param name="symbol">The symbol</param>
        /// <returns>Whether or not the given symbol is subscribed to</returns>
        public bool IsSubscribedTo(String symbol)
        {
            return true;
            // Pseudo-code:
            // - Return true if the symbol is in the subscrition list
        }

        /// <summary>
        /// Initializes this history provider to work for the specified job
        /// </summary>
        /// <param name="job">The job</param>
        /// <param name="mapFileProvider">Provider used to get a map file resolver to handle equity mapping</param>
        /// <param name="factorFileProvider">Provider used to get factor files to handle equity price scaling</param>
        /// <param name="dataProvider">Provider used to get data when it is not present on disk</param>
        /// <param name="statusUpdate">Function used to send status updates</param>
        /// <param name="dataCacheProvider">Provider used to cache history data files</param>
        public void Initialize(AlgorithmNodePacket job, IDataProvider dataProvider, IDataCacheProvider dataCacheProvider, IMapFileProvider mapFileProvider, IFactorFileProvider factorFileProvider, Action<int> statusUpdate)
        {
            return;
        }
        
        /// <summary>
        /// Gets the history for the requested securities
        /// </summary>
        /// <param name="requests">The historical data requests</param>
        /// <param name="sliceTimeZone">The time zone used when time stamping the slice instances</param>
        /// <returns>An enumerable of the slices of data covering the span specified in each request</returns>
        public IEnumerable<Slice> GetHistory(IEnumerable<HistoryRequest> requests, DateTimeZone sliceTimeZone)
        {
            foreach (var request in requests)
            {
                foreach (var slice in ProcessHistoryRequests(request))
                {
                    yield return slice;
                }
            }
        }
        
        /// <summary>
        /// Populate request data
        /// </summary>
        private IEnumerable<Slice> ProcessHistoryRequests(HistoryRequest request)
        {
            return null;
            // TODO: Implement, similar to IEX history provider (code below)
            /*
            var ticker = request.Symbol.ID.Symbol;
            var start = request.StartTimeUtc.ConvertFromUtc(TimeZones.NewYork);
            var end = request.EndTimeUtc.ConvertFromUtc(TimeZones.NewYork);

            if (request.Resolution != Resolution.Daily)
            {
                Log.Error("IEXDataQueueHandler.GetHistory(): History calls for IEX only support daily resolution.");
                yield break;
            }
            if (start <= DateTime.Today.AddYears(-5))
            {
                Log.Error("IEXDataQueueHandler.GetHistory(): History calls for IEX only support a maximum of 5 years history.");
                yield break;
            }

            Log.Trace(string.Format("IEXDataQUeueHandler.ProcessHistoryRequests(): Submitting request: {0}-{1}: {2} {3}->{4}", request.Symbol.SecurityType, ticker, request.Resolution, start, end));

            var client = new WebClient();
            client.Headers.Add("user-agent", "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36");
            var response = client.DownloadString("https://api.iextrading.com/1.0/stock/" + ticker + "/chart/5y");
            var parsedResponse = JArray.Parse(response);

            foreach (var item in parsedResponse.Children())
            {
                var date = DateTime.Parse(item["date"].Value<string>());

                if (date.Date < start.Date || date.Date > end.Date)
                {
                    continue;
                }

                Interlocked.Increment(ref _dataPointCount);

                var open = item["open"].Value<decimal>();
                var high = item["high"].Value<decimal>();
                var low = item["low"].Value<decimal>();
                var close = item["close"].Value<decimal>();
                var volume = item["volume"].Value<int>();

                TradeBar tradeBar = new TradeBar(date, request.Symbol, open, high, low, close, volume);

                yield return new Slice(tradeBar.EndTime, new[] { tradeBar });
            }
            */
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void ProcessJsonObject(JObject message)
        {
          // TODO: Implement, similar to IEX history provider (code below)
          /*try
          {
              // https://iextrading.com/developer/#tops-tops-response
              var symbolString = message["symbol"].Value<string>();
              Symbol symbol;
              if (!_symbols.TryGetValue(symbolString, out symbol))
              {
                  if (_subscribedToAll)
                  {
                      symbol = Symbol.Create(symbolString, SecurityType.Equity, Market.USA);
                  }
                  else
                  {
                      Log.Trace("IEXDataQueueHandler.ProcessJsonObject(): Received unexpected symbol '" + symbolString + "' from IEX in IEXDataQueueHandler");
                      return;
                  }
              }
              var bidSize = message["bidSize"].Value<long>();
              var bidPrice = message["bidPrice"].Value<decimal>();
              var askSize = message["askSize"].Value<long>();
              var askPrice = message["askPrice"].Value<decimal>();
              var volume = message["volume"].Value<int>();
              var lastSalePrice = message["lastSalePrice"].Value<decimal>();
              var lastSaleSize = message["lastSaleSize"].Value<int>();
              var lastSaleTime = message["lastSaleTime"].Value<long>();
              var lastSaleDateTime = UnixEpoch.AddMilliseconds(lastSaleTime);
              var lastUpdated = message["lastUpdated"].Value<long>();
              if (lastUpdated == -1)
              {
                  // there were no trades on this day
                  return;
              }
              var lastUpdatedDatetime = UnixEpoch.AddMilliseconds(lastUpdated);

              var tick = new Tick()
              {
                  Symbol = symbol,
                  Time = lastUpdatedDatetime.ConvertFromUtc(TimeZones.NewYork),
                  TickType = lastUpdatedDatetime == lastSaleDateTime ? TickType.Trade : TickType.Quote,
                  Exchange = "IEX",
                  BidSize = bidSize,
                  BidPrice = bidPrice,
                  AskSize = askSize,
                  AskPrice = askPrice,
                  Value = lastSalePrice,
                  Quantity = lastSaleSize
              };
              _outputCollection.TryAdd(tick);
          }
          catch (Exception err)
          {
              // this method should never fail
              Log.Error("IEXDataQueueHandler.ProcessJsonObject(): " + err.Message);
          }*/
        }
    }
}
