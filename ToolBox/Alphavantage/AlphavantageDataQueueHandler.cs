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

namespace QuantConnect.ToolBox.Alphavantage
{
    /// <summary>
    /// Live Data Queue is the cut out implementation of how to bind a custom live data source 
    /// </summary>
    public class AlphavantageDataQueueHandler : LiveDataQueue
    {
        private Dictionary<Symbol, DateTime> _subscribedSymbols; // List of subscribed symbols. and the timestamp of the last data received for this symbol.
        private string _apiToken;
        private bool _apiTokenIsValid;

        public AlphavantageDataQueueHandler()
        {
            _apiTokenIsValid = false;
            _apiToken = Config.Get("alphavantage-api-access-token");
            _subscribedSymbols = new Dictionary<Symbol, DateTime>();
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
        /// Desktop/Local doesn't support live data from this handler
        /// </summary>
        /// <returns>Tick</returns>
        public sealed override IEnumerable<BaseData> GetNextTicks()
        {
            
            // Pseudo-code
            /*
            - Iterate through the subscribed symbols
                - Query the data from Alphavantage
                - Add to the return: Each minute data that is later than the last tick received.
                    - If no tick has been received, return all ticks that are later than one resolution cycle ago.
                      Ex: If we requested hourly data for SPY symbol, return all data from the past hour. If not first tick received, return all data that is later than last tick received.
            */
            if (_apiTokenIsValid)
            {
                List<Symbol> subscribedSymbols = new List<Symbol>(_subscribedSymbols.Keys);
                foreach (Symbol subscription in subscribedSymbols)
                {
                    var sb = new StringBuilder();
                    sb.Append("https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY");
                    sb.Append("&symbol=" + subscription.Value);
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
                    var response = client.DownloadString(sb.ToString());
                    var parsedResponse = JObject.Parse(response);

                    if (parsedResponse["Time Series (1min)"] != null)
                    {
                        JObject data = (JObject)parsedResponse["Time Series (1min)"];
                        DateTime latestTimeStamp = _subscribedSymbols[subscription];
                        foreach (var item in data)
                        {
                            var timestampString = item.Key;
                            var timestamp = DateTime.Parse(timestampString);

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

                            Tick tick = new Tick(timestamp, subscription, close, close, close);
                            tick.Quantity = volume;

                            //(timestamp, subscription, open, high, low, close, volume);

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
