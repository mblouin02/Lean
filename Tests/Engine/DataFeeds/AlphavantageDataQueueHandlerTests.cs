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
 *
*/

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using QuantConnect.Configuration;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Logging;
using QuantConnect.Securities;
using QuantConnect.ToolBox.Alphavantage;

namespace QuantConnect.Tests.Engine.DataFeeds
{
    [TestFixture]
    public class AlphavantageDataQueueHandlerTests
    {

        [Test]
        public void AlphavantageCouldSubscribe()
        {
            var alphavantage = new AlphavantageDataQueueHandler();

            alphavantage.Subscribe(null, new[]
            {
                Symbol.Create("SPY", SecurityType.Equity, Market.USA)
            });

            Assert.IsTrue(alphavantage.IsSubscribedTo("SPY"));
        }

        [Test]
        public void AlphavantageCouldSubscribeTwiceSameSymbolWithoutError()
        {
            var alphavantage = new AlphavantageDataQueueHandler();

            alphavantage.Subscribe(null, new[]
            {
                Symbol.Create("SPY", SecurityType.Equity, Market.USA)
            });
            alphavantage.Subscribe(null, new[]
            {
                Symbol.Create("SPY", SecurityType.Equity, Market.USA)
            });

            Assert.IsTrue(alphavantage.IsSubscribedTo("SPY"));
        }

        [Test]
        public void AlphavantageCouldSubscribeAndUnsubscribe()
        {
            var alphavantage = new AlphavantageDataQueueHandler();

            alphavantage.Subscribe(null, new[]
            {
                Symbol.Create("SPY", SecurityType.Equity, Market.USA)
            });
            alphavantage.Subscribe(null, new[]
            {
                Symbol.Create("AAPL", SecurityType.Equity, Market.USA)
            });
            alphavantage.Subscribe(null, new[]
            {
                Symbol.Create("AMZN", SecurityType.Equity, Market.USA)
            });

            Assert.IsTrue(alphavantage.IsSubscribedTo("SPY"));
            Assert.IsTrue(alphavantage.IsSubscribedTo("AAPL"));
            Assert.IsTrue(alphavantage.IsSubscribedTo("AMZN"));

            alphavantage.Unsubscribe(null, new[]
            {
                Symbol.Create("SPY", SecurityType.Equity, Market.USA)
            });
            alphavantage.Unsubscribe(null, new[]
            {
                Symbol.Create("AAPL", SecurityType.Equity, Market.USA)
            });

            Assert.IsFalse(alphavantage.IsSubscribedTo("SPY"));
            Assert.IsFalse(alphavantage.IsSubscribedTo("AAPL"));
            Assert.IsTrue(alphavantage.IsSubscribedTo("AMZN"));
        }

        [Test]
        public void AlphavantageCouldGetLiveData()
        {
            var alphavantage = new AlphavantageDataQueueHandler();
            string token = Config.Get("alphavantage-api-access-token");
            Assert.IsTrue(token != "");

            alphavantage.Subscribe(null, new[]
            {
                Symbol.Create("SPY", SecurityType.Equity, Market.USA)
            });
            alphavantage.Subscribe(null, new[]
            {
                Symbol.Create("AAPL", SecurityType.Equity, Market.USA)
            });

            IEnumerable<BaseData> data = alphavantage.GetNextTicks();

            int dataCount = 0;
            foreach (BaseData dataPoint in data)
            {
                Assert.True(dataPoint.Symbol == Symbol.Create("SPY", SecurityType.Equity, Market.USA) || dataPoint.Symbol == Symbol.Create("AAPL", SecurityType.Equity, Market.USA));
                Assert.True(dataPoint.DataType == MarketDataType.Tick);
                Assert.True(dataPoint.EndTime <= DateTime.UtcNow);
                Assert.True(dataPoint.Time <= DateTime.UtcNow);
                Assert.True(dataPoint.Price > 0);
                Assert.True(dataPoint.Value > 0);

                if (dataCount <= 10)
                {
                    Log.Trace("{0}: {1} - Value={2}", dataPoint.Time, dataPoint.Symbol.Value, dataPoint.Value);
                }

                dataCount++;
            }
            Assert.GreaterOrEqual(dataCount, 2);
        }
    }
}