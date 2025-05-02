const net = require('net');
const config = require('./config.json');
const cache = new Map();
const CACHE_LENGTH = 600000;

let verbose = false;

function clog(...args) {
  if (verbose) {
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
  if (Date.now() - item.time > CACHE_LENGTH) {
    cache.delete(key);
    return null;
  }
  clog('getting value from cache', clientName, key);
  return item.chunks;
}

function clearOldCache() {
  for (const [key, item] of cache) {
    if (Date.now() - item.time > CACHE_LENGTH) {
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
          return;
        }
      }

      if (!returnedChunks) {
        const redisSocket = net.createConnection({ host: redisHost, port: redisPort }, () => {
          redisSocket.write(data);
        });

        redisSocket.on('data', (chunk) => {
          pushIntoCache(clientName, key, chunk);
          client.write(chunk)
        });
        redisSocket.on('error', err => {
          console.error('Redis error:', err.message);
          client.write(`-Redis error\r\n`);
        });
        redisSocket.on('close', () => {
        });
      }
    });

    client.on('error', err => console.error('Client error:', err.message));
  });

  clientObj.clientInstance.listen(relayPort, () => {
    clog(`${clientName}: Redis relay with cache on port ${relayPort}, target ${redisHost}:${redisPort}`);
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

setInterval(() => {
  clog(voiceTotalCalls());
}, 1000);


/// listen to keyboard and clear cache

process.stdin.on('data', (data) => {
  let key = bufferToHashString(data);

  if (key === 'help') {
    console.log('Commands: cache, exit, counter, stats, verbose, clear');
  }

  if (key === 'clear') {
    process.stdout.write('\x1B[2J\x1B[0f');
  }

  if (key === 'verbose') {
    verbose = !verbose;
  }

  if (key === 'cache') {
    cache.clear();
  }

  if (key === 'stats') {
    console.log(voiceTotalCalls());
  }

  if (key === 'exit') {
    process.exit();    
  } 

  if (key === 'counter') {
    for (const client of config) {
      client.counter = 0;
    }
  }

});
