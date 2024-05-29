import Big from "big.js";
import WebSocket from "ws";
import fetch from 'node-fetch';
import { S3Client } from "@aws-sdk/client-s3";
import { Upload } from '@aws-sdk/lib-storage';
import { JsonStreamStringify } from 'json-stream-stringify';
import zlib from 'zlib';
import dotenv from 'dotenv';
dotenv.config();

// Parse parameters
const [ _base, _quote ] = process.argv.slice(2);
const base = _base.toUpperCase();
const quote = _quote.toUpperCase();

const is_test = (process.argv[4] === 'test');

const client = new S3Client({
  region: process.env['AWS-S3_REGION'], 
  credentials: {
    accessKeyId: process.env['AWS-S3_ACCESS_KEY_ID'],
    secretAccessKey: process.env['AWS-S3_SECRET_ACCESS_KEY'],
  }
});

async function CompressAndSendBigJSONToS3 (name, bigjson) {
  const compress_stream = zlib.createGzip();
  new JsonStreamStringify(bigjson).pipe(compress_stream);

  const upload = new Upload({
    client,
    params: {
      Bucket: process.env['AWS-S3_BUCKET_NAME'],
      Key: name+'.gz',
      Body: compress_stream,
    }
  });

  await upload.done();
}

const Market = "PF_" + [ base, quote ].join('');
const full_market_name =  `kraken-futures ${base}-${quote}`;
const no_delay_attemps = 3;
const reconn_delay = 15e3;
const book_resync_time = 5*60e3; // 5 min
const max_snapshot_depth = 500;
const to_save_depth = is_test ? 5 : 50;
const seconds_to_export = 10;
const num_of_connections = 3;

let connections = [];
let connections_size = 0;

const dict = {
  bside: { 'buy': 'bids', 'sell': 'asks' },
  bcmp: { 'buy': 'gte', 'sell': 'lte' }
}

let data_time = null;
let seconds_data = [];

async function Request (path) {
  const r = await fetch('https://futures.kraken.com' + path);
  if (r.status !== 200)
    throw '[E] Requesting "' + path + '": (' + r.status + ') ' + await r.text();
  return r.json();
}

async function ValidateMarket () {
  const { tickers }  = await Request('/derivatives/api/v3/tickers');
  const ticker = tickers.find(({ symbol }) => symbol === Market);

  // Check if it is a valid market.
  if (ticker == null)
    throw '[E] ValidateMarket: Unknow "' + Market + '" market.';

  // Check if market is active.
  if (ticker.suspended !== false)
    throw '[E] ValidateMarket: "' + Market + '" market is suspended.'
}

function NewUpdateSec (conn, update_time, update_sec) {
  if (data_time == null) data_time = update_sec - 1;
  
  if (!is_test)
    console.log('[!] New second, book_sec ('+Math.floor(this.orderbook.timestamp / 1e3)+') upd_sec ('+update_sec+') { '+((Date.now() - update_time) / 1e3).toFixed(3)+' sec delay }');

  while (data_time < update_sec) {
    const obj = {
      asks: conn.orderbook.asks.slice(0, to_save_depth),
      bids: conn.orderbook.bids.slice(0, to_save_depth),
      book_timestamp: conn.orderbook.timestamp,
      second: ++data_time,
      trades: conn.trades.filter(t => 
        Big(t.timestamp).gt((data_time - 1) * 1e3) &&
        Big(t.timestamp).lte(data_time * 1e3)
      ),
    };
    
    if (is_test)
      console.log(obj);
    else
      seconds_data.push(obj);
  }

  if (data_time % seconds_to_export == 0) SaveToS3();
}

async function SaveToS3 () {
  if (is_test) return;
  
  // Create a name to the file being saved.
  const timestr = new Date((data_time - 60*60*3) * 1e3).toISOString().slice(0, 16).replaceAll(':', '-');
  const name =  full_market_name.replace(' ', '_') + '_' + timestr + '.json';

  // Compress data then save it.
  CompressAndSendBigJSONToS3(name, seconds_data)
  .then(() => console.log('[!] Data saved successfuly.'))
  .catch(error => console.log('[E] Failed to save data:',error));
  
  // Reset data in memory. 'seconds_data'.
  seconds_data = [];
}

class Connection {
  constructor () {
    this.conn_id = connections_size++;
    this.failed_attemps = 0;
  }

  async SyncMarket () {
    this.syncProm = new Promise((resolve, reject) => { 
      this.syncPromFuncs = { resolve, reject };
    })
    .finally(() => {
      delete this.syncPromFuncs;
      delete this.syncProm;
    });
    
    this.last_conn_attemp = Date.now();
    const ws = new WebSocket("wss://futures.kraken.com/ws/v1");
  
    ws.on('open', () => {
      console.log('[!] (' + this.conn_id + ') WebSocket opened.');

      // Keep the connection alive with ping loop.
      this.pingLoopInterval = setInterval(() => ws.ping(), 30e3);

      // Subscribe to trades.
      ws.send(JSON.stringify({
        event: "subscribe",
        feed: "trade",
        product_ids: [ Market ]
      }));

      // Subscribe to orderbook.
      ws.send(JSON.stringify({
        event: "subscribe",
        feed: "book",
        product_ids: [ Market ]
      }));
    });
    
    ws.on('error', err => {
      console.log('[E] (' + this.conn_id + ') WebSocket:',err);
      ws.terminate();
    });
    
    ws.on('close', async () => {
      this.trades = null;
      this.orderbook = null;
      clearInterval(this.pingLoopInterval);

      console.log('[!] (' + this.conn_id + ') WebSocket closed.');

      if (Date.now() - this.last_conn_attemp < reconn_delay && ++this.failed_attemps > no_delay_attemps) {
        console.log('/!\\ (' + this.conn_id + ') Trying reconnection in ' + Math.ceil(reconn_delay / 1e3) + ' second(s).');
        await new Promise(r => setTimeout(r, reconn_delay));
      }

      console.log('/!\\ (' + this.conn_id + ') Reconnecting with WebSocket...');
      this.SyncMarket();
    });
  
    ws.on('ping', data => ws.pong(data));
    
    ws.on('message', msg => {
      try {
        msg = msg.toString().replace(/:\s*([-+Ee0-9.]+)/g, ': "$1"'); // Convert numbers to string.
        msg = JSON.parse(msg);
      } catch (error) {
        msg = msg.toString();
      }

      if (msg.event === 'subscribed') {
        if (msg.feed == 'trade')
          return console.log('[!] (' + this.conn_id + ') Subscribed to trades.');

        if (msg.feed == 'book')
          return console.log('[!] (' + this.conn_id + ') Subscribed to orderbook.');
      }

      if (msg.feed === 'book') {
        const seq = parseInt(msg.seq);
        if (seq != this.orderbook.seq + 1) {
          // Resync orderbook.
          console.log('[E] wrong book, update sequence is wrong.');
          process.exit(1);
        }

        const update_time = parseInt(msg.timestamp);
        const update_sec = Math.floor(update_time / 1e3);
        const book_sec = Math.floor(this.orderbook.timestamp / 1e3);

        // Check if is a new second.
        if (update_sec > book_sec && (data_time == null || update_sec > data_time))
          NewUpdateSec(this, update_time, update_sec);

        const bside = dict.bside[msg.side];
        const price = Big(msg.price);
        const amount = Big(msg.qty);
        const idx = this.orderbook[bside].findIndex(bOrd => price[dict.bcmp[msg.side]](bOrd[0]));

        if (amount.eq(0)) {
          if (idx != -1 && price.eq(this.orderbook[bside][idx][0])) {
            // Remove price level.
            this.orderbook[bside].splice(idx, 1);
            
          }
        } else {
          if (idx == -1) {
            // Push to the end of the book side.
            this.orderbook[bside].push([ price.toFixed(), amount.toFixed() ]);
          
          } else if (price.eq(this.orderbook[bside][idx][0])) {
            // Updates existing price level.
            this.orderbook[bside][idx][1] = amount.toFixed();

          } else {
            // Push adds new price level.
            this.orderbook[bside].splice(idx, 0, [ price.toFixed(), amount.toFixed() ]);

          }
        }

        if (Big(this.orderbook.asks[0][0]).lte(this.orderbook.bids[0][0])) {
          // Resync orderbook.
          console.log('[E] wrong book, best ask lower than or equal best bid.');
          process.exit(1);
        }

        this.orderbook.seq = seq;
        this.orderbook.timestamp = update_time;

        // Check if is time to resync the orderbook.
        if (this.orderbook._resyncing !== true && Date.now() - this.orderbook.last_snapshot_time > book_resync_time) {
          this.orderbook._resyncing = true;
          ws.send(JSON.stringify({
            event: "subscribe",
            feed: "book",
            product_ids: [ Market ]
          }));
        }

        return;
      }

      if (msg.feed === 'trade') {
        const nowms = Date.now();
        this.trades.push({
          timestamp: parseInt(msg.time),
          is_buy: (msg.side === 'buy'),
          price: Big(msg.price).toFixed(),
          amount: Big(msg.qty).toFixed()
        });
        this.trades = this.trades.filter(t => nowms - t.timestamp <= 10e3); // Keep only the last 10 seconds
        return;
      }

      if (msg.feed === 'book_snapshot') {
        this.orderbook = {
          asks: msg.asks.slice(0, max_snapshot_depth).map(({ price, qty }) => [ Big(price).toFixed(), Big(qty).toFixed() ]),
          bids: msg.bids.slice(0, max_snapshot_depth).map(({ price, qty }) => [ Big(price).toFixed(), Big(qty).toFixed() ]),
          seq: parseInt(msg.seq),
          timestamp: parseInt(msg.timestamp),
          last_snapshot_time: Date.now()
        };
        // console.log('orderbook:',this.orderbook);

        if (this.syncPromFuncs && this.trades != null)
          this.syncPromFuncs.resolve(); 

        return;
      }

      if (msg.feed === 'trade_snapshot') {
        const nowms = Date.now();
        this.trades = msg.trades
          .reverse()
          .map(t => ({
            timestamp: parseInt(t.time),
            is_buy: (t.side === 'buy'),
            price: Big(t.price).toFixed(),
            amount: Big(t.qty).toFixed()
          }))
          .filter(t => nowms - t.timestamp <= 10e3); // Keep only the last 10 seconds
        // console.log('trades:',this.trades);

        if (this.syncPromFuncs && this.orderbook != null)
          this.syncPromFuncs.resolve();
        
        return;
      }

      if (msg.event === 'alert' && msg.message === 'Already subscribed to feed, re-requesting') return;

      if (msg.event === 'info') return;
  
      console.log('/!\\ (' + this.conn_id + ') WebSocket message unhandled:',msg);
    });
  
    return this.syncProm;
  };
};

(async () => {
  await ValidateMarket();
  console.log('[!] Market is valid.');

  // Create and sync with 3 connections
  while (connections_size < num_of_connections)
    connections.push(new Connection());

  // Sync with the market using all the connections.
  await Promise.all(connections.map(c => c.SyncMarket()));
})();