const net = require('net');
const config = require('./config.json');
const cache = new Map();
const CACHE_LENGTH = 600000;

function isGet(buffer) {
  const lines = buffer.toString().split('\r\n');
  if (lines[2]?.toUpperCase().indexOf('GET') > -1) {
    return true;
  }
  return false;
}

function bufferToHashString(data) {
  return data.toString().split('\r\n').map(line => line.split(':')[1]).join(':');
}

function buildBulkString(value) {
  if (value === null) return `$-1\r\n`;
  return `$${value.length}\r\n${value}\r\n`;
}

function pushIntoCache(clientName, key, value) {
  let item = cache.get(`${clientName}:${key}`);
  if (!item) {
    createKeyCache(clientName, key);
    item = cache.get(`${clientName}:${key}`);
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
  let item = cache.get(key);
  if (!item) return null;
  if (Date.now() - item.time > CACHE_LENGTH) {
    cache.delete(key);
    return null;
  }
  return item.val;
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
      if (isGet(data)) {
        let chunks = getCacheChunks(clientName, key);
        if (chunks) {
          console.log('Chunks:', chunks.length);
          for (const chunk of chunks) {
            client.write(chunk);
          }
          return;
        }
      }

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

setInterval(() => {
  console.log(voiceTotalCalls());
}, 1000);


