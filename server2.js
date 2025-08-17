{
  "name": "pettusplots-data-proxy",
  "version": "1.0.0",
  "description": "PettusPlots data proxy server that converts weatherwise .wise files to .plots format with WebSocket support",
  "main": "server2.js",
  "scripts": {
    "start": "node server2.js",
    "dev": "node server2.js",
    "test": "echo \"No tests specified\" && exit 0"
  },
  "keywords": [
    "weather",
    "satellite",
    "proxy",
    "websocket",
    "pettusplots"
  ],
  "author": "James Pettus",
  "license": "MIT",
  "dependencies": {
    "express": "^4.18.2",
    "ws": "^8.14.2",
    "node-fetch": "^2.7.0"
  },
  "engines": {
    "node": ">=16.0.0"
  },
  "repository": {
    "type": "git",
    "url": "https://github.com/jamespettus/pettusplots"
  },
  "homepage": "https://pettusplots.com"
}
