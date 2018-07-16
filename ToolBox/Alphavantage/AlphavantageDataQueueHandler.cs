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
using QuantConnect.Configuration;
using System.Text;
using System.Net;
using QuantConnect.Data.Market;
using QuantConnect.Logging;
using System.Threading;

namespace QuantConnect.ToolBox.Alphavantage
{
    /// <summary>
    /// Live Data Queue is the cut out implementation of how to bind a custom live data source 
    /// </summary>
    public class AlphavantageDataQueueHandler : LiveDataQueue, IHistoryProvider
    {
        private Dictionary<Symbol, DateTime> _subscribedSymbols; // List of subscribed symbols. and the timestamp of the last data received for this symbol.
        private string _apiToken;
        private bool _apiTokenIsValid;
        private int _dataPointCount;
        private DateTime _lastApiCallTime = DateTime.MinValue;
        private List<Symbol> _apiRequestsQueue;

        /// <summary>
        /// Gets the total number of data points emitted by this history provider
        /// </summary>
        public int DataPointCount
        {
            get { return _dataPointCount; }
        }

        public AlphavantageDataQueueHandler()
        {
            _apiTokenIsValid = false;
            _apiToken = Config.Get("alphavantage-api-access-token");
            _subscribedSymbols = new Dictionary<Symbol, DateTime>();
            _apiRequestsQueue = new List<Symbol>();
            if (_apiToken.Length >= 5)
            {
                _apiTokenIsValid = true;
            }
            else
            {
                throw new ArgumentException("Alphavantage api token invalid. Please check value 'alphavantage-api-access-token' in config.json");
            }
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
            if (_apiTokenIsValid)
            {
                foreach (var request in requests)
                {
                    foreach (var slice in ProcessHistoryRequests(request))
                    {
                        yield return slice;
                    }
                }
            }
        }

        /// <summary>
        /// Populate request data
        /// </summary>
        private IEnumerable<Slice> ProcessHistoryRequests(HistoryRequest request)
        {
            var ticker = request.Symbol.ID.Symbol;
            var start = request.StartTimeUtc.ConvertFromUtc(TimeZones.NewYork);
            var end = request.EndTimeUtc.ConvertFromUtc(TimeZones.NewYork);

            if (request.Resolution != Resolution.Daily)
            {
                Log.Error("AlphavantageDataQueueHandler.GetHistory(): History calls for Alphavantage only support daily resolution.");
                yield break;
            }

            Log.Trace(string.Format("AlphavantageDataQueueHandler.ProcessHistoryRequests(): Submitting request: {0}-{1}: {2} {3}->{4}", request.Symbol.SecurityType, ticker, request.Resolution, start, end));

            var sb = new StringBuilder();
            sb.Append("https://www.alphavantage.co/query?function=TIME_SERIES_DAILY");
            sb.Append("&symbol=" + ticker);
            sb.Append("&outputsize=full");
            sb.Append("&apikey=" + _apiToken);

            var client = new WebClient();
            client.Headers.Add("user-agent", "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36");
            var response = client.DownloadString(sb.ToString());
            var parsedResponse = JObject.Parse(response);

            if (parsedResponse["Time Series (Daily)"] != null)
            {
                JObject data = (JObject)parsedResponse["Time Series (Daily)"];

                foreach (var item in data)
                {
                    var timestampString = item.Key;
                    var timestamp = DateTime.Parse(timestampString);

                    if (timestamp.Date < start.Date || timestamp.Date > end.Date)
                    {
                        continue;
                    }
                    Interlocked.Increment(ref _dataPointCount);

                    var open = item.Value["1. open"].Value<decimal>();
                    var high = item.Value["2. high"].Value<decimal>();
                    var low = item.Value["3. low"].Value<decimal>();
                    var close = item.Value["4. close"].Value<decimal>();
                    var volume = item.Value["5. volume"].Value<int>();

                    TradeBar tradeBar = new TradeBar(timestamp.Date, request.Symbol, open, high, low, close, volume);

                    yield return new Slice(tradeBar.EndTime, new[] { tradeBar });
                }
            }
            else
            {
                throw new ArgumentException("QuantConnect.Toolbox.AlphavantageDataQueueHandler: Invalid API call, or API call limit reached for this minute.");
            }
        }

        /// <summary>
        /// Desktop/Local doesn't support live data from this handler
        /// </summary>
        /// <returns>Tick</returns>
        public sealed override IEnumerable<BaseData> GetNextTicks()
        {
            Log.Trace("AlphavantageDataQueueHandler.GetNextTicks(): Start");

            if (_apiTokenIsValid && ((DateTime.Now.Minute != _lastApiCallTime.Minute && DateTime.Now.Second >= 30) || _lastApiCallTime == DateTime.MinValue))
            {
                _lastApiCallTime = DateTime.Now;

                foreach(Symbol subscription in _subscribedSymbols.Keys)
                {
                    if(!_apiRequestsQueue.Contains(subscription))
                    {
                        _apiRequestsQueue.Add(subscription);
                    }
                }

                List<Symbol> subscribedSymbols = new List<Symbol>(_apiRequestsQueue);

                foreach (Symbol subscription in subscribedSymbols)
                {
                    if (subscription.Contains("QC-UNIVERSE"))
                    {
                        continue;
                    }

                    Log.Trace("AlphavantageDataQueueHandler.GetNextTicks(): Processing subscription: " + subscription);
                    Log.Trace("AlphavantageDataQueueHandler.GetNextTicks(): Subscription " + subscription + " is of type: " + subscription.SecurityType);
                    var sb = new StringBuilder();
                    sb.Append("https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY");
                    sb.Append("&symbol=" + subscription);
                    sb.Append("&interval=1min");
                    if (_subscribedSymbols[subscription] == DateTime.MinValue)
                    {
                        sb.Append("&outputsize=full");
                    }
                    else
                    {
                        sb.Append("&outputsize=compact");
                    }
                    sb.Append("&apikey=" + _apiToken);

                    var client = new WebClient();
                    client.Headers.Add("user-agent", "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36");

                    Log.Trace("AlphavantageDataQueueHandler.GetNextTicks(): Sending this request:" + sb.ToString());

                    var response = client.DownloadString(sb.ToString());
                    var parsedResponse = JObject.Parse(response);

                    Log.Trace("AlphavantageDataQueueHandler.GetNextTicks(): Received response. Checking if it contains 'Time Series (1min)'");
                    //Log.Trace("AlphavantageDataQueueHandler.GetNextTicks(): Response = " + response);

                    if (parsedResponse["Time Series (1min)"] != null)
                    {
                        Log.Trace("AlphavantageDataQueueHandler.GetNextTicks(): Found 'Time Series (1min)'");

                        _apiRequestsQueue.Remove(subscription);

                        JObject data = (JObject)parsedResponse["Time Series (1min)"];
                        DateTime latestTimeStamp = _subscribedSymbols[subscription];

                        // TODO: Remove this isFirstItem debug check
                        bool isFirstItem = true;
                        bool isFirstValidItem = true;

                        Log.Trace("AlphavantageDataQueueHandler.GetNextTicks(): About to iterate through all data items received");
                        foreach (var item in data)
                        {
                            var timestampString = item.Key;
                            var timestamp = DateTime.Parse(timestampString);

                            if (isFirstItem)
                            {
                                Log.Trace("AlphavantageDataQueueHandler.GetNextTicks(): First data item received: " + timestampString);
                                isFirstItem = false;
                            }

                            if (timestamp <= _subscribedSymbols[subscription])
                            {
                                continue;
                            }
                            latestTimeStamp = timestamp;

                            var open = item.Value["1. open"].Value<decimal>();
                            var high = item.Value["2. high"].Value<decimal>();
                            var low = item.Value["3. low"].Value<decimal>();
                            var close = item.Value["4. close"].Value<decimal>();
                            var volume = item.Value["5. volume"].Value<int>();

                            if (isFirstValidItem)
                            {
                                Log.Trace("AlphavantageDataQueueHandler.GetNextTicks(): First valid data item received: ");
                                Log.Trace("-------- Timestamp = " + timestampString);
                                Log.Trace("-------- Open = " + open);
                                Log.Trace("-------- High = " + high);
                                Log.Trace("-------- Low = " + low);
                                Log.Trace("-------- Close = " + close);
                                Log.Trace("-------- Volume = " + volume);
                                isFirstValidItem = false;
                            }

                            Tick tick = new Tick(timestamp, subscription, close, close, close);
                            tick.Quantity = volume;
                            tick.TickType = TickType.Trade;

                            yield return tick;
                        }
                        _subscribedSymbols[subscription] = latestTimeStamp;
                    }
                }
            }
        }

        /// <summary>
        /// Adds the symbol to the list of subscriptions
        /// </summary>
        public sealed override void Subscribe(LiveNodePacket job, IEnumerable<Symbol> symbols)
        {
            foreach (Symbol symbol in symbols)
            {
                if (!_subscribedSymbols.ContainsKey(symbol))
                {
                    _subscribedSymbols.Add(symbol, DateTime.MinValue);
                }
            }
        }

        /// <summary>
        /// Removes the symbol from the list of subscriptions
        /// </summary>
        public sealed override void Unsubscribe(LiveNodePacket job, IEnumerable<Symbol> symbols)
        {
            foreach (Symbol symbol in symbols)
            {
                _subscribedSymbols.Remove(symbol);
            }
        }

        /// <summary>
        /// Returns true if the given symbol is subscribed to.
        /// </summary>
        /// <param name="symbol">The symbol</param>
        /// <returns>Whether or not the given symbol is subscribed to</returns>
        public bool IsSubscribedTo(String symbol)
        {
            bool subscribed = false;
            foreach (Symbol subscribedSymbol in _subscribedSymbols.Keys)
            {
                subscribed |= (symbol == subscribedSymbol.Value);
            }
            return subscribed;
        }

        /// <summary>
        /// Returns true if the given symbol is subscribed to.
        /// </summary>
        /// <param name="symbol">The symbol</param>
        /// <returns>Whether or not the given symbol is subscribed to</returns>
        public bool IsSubscribedTo(Symbol symbol)
        {
            return _subscribedSymbols.ContainsKey(symbol);
        }
    }
}
