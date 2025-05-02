const net = require('net');
const config = require('./config.json');
const cache = new Map();
const Parser = require('redis-parser');

let verbose = false;
let extraVerbose = false;
let showCalls = false;
let cacheLength = 600000;

function clog(...args) {
  if (verbose) {
    console.log(...args);
  }
}

function ccall(...args) {
  if (showCalls) {
    console.log(...args);
  }
}

function isGet(buffer) {
  const lines = buffer.toString().split('\r\n');
  if (lines[2]?.toUpperCase().indexOf('GET') > -1) {
    return true;
  }
  return false;
}

function bufferToHashString(data) {
  return data.toString().replace(/\r|\n/g, '');
}

function buildBulkString(value) {
  if (value === null) return `$-1\r\n`;
  return `$${value.length}\r\n${value}\r\n`;
}

function pushIntoCache(clientName, key, value) {
  let item = cache.get(`${clientName}:${key}`);

  clog('storing value', value.length, 'into cache', clientName, key);

  if (!item) {
    createKeyCache(clientName, key);
    item = cache.get(`${clientName}:${key}`);
    item.time = Date.now();
  }

  item.chunks.push(value);
}

function createKeyCache(clientName, key) {
  cache.set(`${clientName}:${key}`, {
    chunks: [],
    time: Date.now()
  });
}

function getCacheChunks(clientName, key) {
  let item = cache.get(`${clientName}:${key}`);
  if (!item) return null;
  if (Date.now() - item.time > cacheLength) {
    cache.delete(key);
    return null;
  }
  clog('getting value from cache', clientName, key);
  return item.chunks;
}

function clearOldCache() {
  for (const [key, item] of cache) {
    if (Date.now() - item.time > cacheLength) {
      cache.delete(key);
    }
  }
}

function createServer(clientObj) {
  const relayPort = clientObj.port;
  const redisHost = clientObj.targetIp;
  const redisPort = clientObj.targetPort;
  const clientName = clientObj.name;
  clientObj.counter = 0;
  clientObj.clientInstance = net.createServer((client) => {
    client.on('data', async (data) => {
      clientObj.counter++;
      let key = bufferToHashString(data);
      let returnedChunks = false;
      if (isGet(data)) {
        let chunks = getCacheChunks(clientName, key);
        if (chunks) {
          for (const chunk of chunks) {
            client.write(chunk);
          }
          returnedChunks = true;
        }
      }

      ccall(`${returnedChunks ? 'hit' : 'miss'} -> C:${clientObj.counter} -> ${clientName} -> ${key}`);


      if (!returnedChunks) {
        const redisSocket = net.createConnection({ host: redisHost, port: redisPort }, () => {
          redisSocket.write(data);
        });
        let bufferChunks = [];

        const parser = new Parser({
          returnBuffers: true,
          returnReply(reply) {
            const fullBuffer = Buffer.concat(bufferChunks);
            pushIntoCache(clientName, key, fullBuffer);
            redisSocket.end(); // 👈 Close connection here
            bufferChunks = [];
          },
          returnError(err) {
            console.error('Redis parse error:', err);
            redisSocket.end();
          }
        });

        redisSocket.on('data', (chunk) => {
          pushIntoCache(clientName, key, chunk);
          client.write(chunk)
          parser.execute(chunk);
        });
        redisSocket.on('error', err => {
          console.error('Redis error:', err.message);
          client.write(`-Redis error\r\n`);
        });
        redisSocket.on('close', () => {
          clog('redis socket closed', clientName);
        });
      }
    });

    client.on('error', err => console.error('Client error:', err.message));
  });

  clientObj.clientInstance.listen(relayPort, () => {
    console.log(`${clientName}: Redis relay with cache on port ${relayPort}, target ${redisHost}:${redisPort}`);
  });
}

for (const client of config) {
  createServer(client);
}

setInterval(clearOldCache, 10000);


function voiceTotalCalls() {
  let total = 0;

  for (const client of config) {
    total += client.counter;
  }
  return total;
}

function voiceExtraTotalCalls() {
  if (extraVerbose) {
    for (const client of config) {
      console.log(`${client.name} -> ${client.counter}`);
    }
  }
}

setInterval(() => {
  if (!verbose) return;
  if (extraVerbose) {
    voiceExtraTotalCalls();
  } else {
    clog(voiceTotalCalls());
  }
}, 1000);


function printHelp() {
  console.log('commands:');
  console.log('  set cache <seconds> -> set cache length in seconds', `(current: ${cacheLength/1000} seconds)`);
  console.log('  reset cache -> clear cache');
  console.log('  reset counter -> reset counter');
  console.log('  stats -> show total tally of calls ');
  console.log('  extra -> show total tally of calls per client');
  console.log('  calls -> show calls per client', `(current: ${showCalls})`);
  console.log('  verbose -> show verbose log', `(current: ${verbose})`);
  console.log('  clear -> clear screen');
  console.log('  help / h -> show this help');
  console.log('  exit -> exit program');

  return;
}

printHelp();

/// listen to keyboard and clear cache

process.stdin.on('data', (data) => {
  //auto complete

  let key = bufferToHashString(data);

  if (key === 'help' || key === 'h') {
    printHelp();
  }

  if (key === 'clear') {
    process.stdout.write('\x1B[2J\x1B[0f');
    printHelp();
  }

  if (key === 'verbose') {
    verbose = !verbose;
    console.log('verbose:', verbose);
  }

  if (key === 'reset cache') {
    cache.clear();
    console.log('cache cleared');
  }

  if (key === 'stats') {
    if (extraVerbose) {
      voiceExtraTotalCalls();
    } else {
      console.log(voiceTotalCalls());
    }
  }

  if (key === 'exit') {
    process.exit();    
  } 

  if (key === 'reset counter') {
    for (const client of config) {
      client.counter = 0;
    }
    console.log('counters reset'); 
  }

  if (key === 'extra') {
    extraVerbose = !extraVerbose;
    console.log('extra verbose:', extraVerbose);
  }

  if (key === 'calls') {
    showCalls = !showCalls;
    console.log('show calls:', showCalls);
  }

  if (key.indexOf('set cache') > -1) {
    try {
      const seconds = parseInt(key.split(' ')[2]);

      if (isNaN(seconds)) {
        console.log('invalid cache length');
        return;
      }

      cacheLength = seconds * 1000;
      console.log('cache length set to', seconds, 'seconds');
    } catch (err) {
      console.log('invalid cache length');
    }
  }

});
