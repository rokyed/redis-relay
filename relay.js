// redis-relay-cache.js
const net = require('net');
const { createClient } = require('redis');

// Parse CLI arguments
const [,, relayPortArg, redisHostArg, redisPortArg] = process.argv;
const relayPort = parseInt(relayPortArg, 10) || 6380;
const redisHost = redisHostArg || '127.0.0.1';
const redisPort = parseInt(redisPortArg, 10) || 6379;

// Cache setup
const cache = new Map();
const now = () => Date.now();
const setCache = (key, value) => {
  cache.set(key, { value, expiry: now() + 60_000 });
};
const getCache = (key) => {
  const entry = cache.get(key);
  if (entry && entry.expiry >= now()) return entry.value;
  cache.delete(key);
  return null;
};

// Redis client for internal queries
const redis = createClient({ url: `redis://${redisHost}:${redisPort}` });
redis.connect().catch(console.error);

// RESP simple parser (only for GET commands)
function parseCommand(buffer) {
  const lines = buffer.toString().split('\r\n');
  if (lines[2]?.toUpperCase() === 'GET') {
    return lines[4]; // key is typically at line 4
  }
  return null;
}

// Minimal RESP response builder
function buildBulkString(value) {
  if (value === null) return `$-1\r\n`;
  return `$${value.length}\r\n${value}\r\n`;
}

const server = net.createServer((client) => {
  client.on('data', async (data) => {
    const key = parseCommand(data);

    if (key) {
      const cached = getCache(key);
      if (cached !== null) {
        console.log('Redis cache hit for', key);
        client.write(buildBulkString(cached));
        return;
      }

      // Not cached, get from real Redis
      try {
        const val = await redis.get(key);
        setCache(key, val);
        client.write(buildBulkString(val));
      } catch (err) {
        console.error('Redis GET error:', err);
        client.write(`-Error fetching from Redis\r\n`);
      }
    } else {
      // Pipe non-GET commands directly
      const redisSocket = net.createConnection({ host: redisHost, port: redisPort }, () => {
        redisSocket.write(data);
      });

      redisSocket.on('data', chunk => client.write(chunk));
      redisSocket.on('error', err => {
        console.error('Redis error:', err.message);
        client.write(`-Redis error\r\n`);
      });
    }
  });

  client.on('error', err => console.error('Client error:', err.message));
});

server.listen(relayPort, () => {
  console.log(`Redis relay with cache on port ${relayPort}, target ${redisHost}:${redisPort}`);
});

