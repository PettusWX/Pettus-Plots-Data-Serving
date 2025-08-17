const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const fetch = require('node-fetch');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });
const PORT = process.env.PORT || 3001;

// Weatherwise WebSocket and HTTP endpoints
const WEATHERWISE_WS_URL = 'wss://api.weatherwise.app';
const WEATHERWISE_HTTP_ENDPOINTS = [
    'https://data2.weatherwise.app',
    'https://data1.weatherwise.app'
];

// WebSocket connection to weatherwise
let weatherwiseWS = null;
const connectedClients = new Set();

// All supported products for automatic subscription
const SUPPORTED_PRODUCTS = {
    satellite: [
        // GOES-19 (East)
        { satellite: 'GOES-19', region: 'CONUS', channels: ['ABI-L1b-C13', 'ABI-L1b-C14', 'ABI-L1b-C03', 'ABI-L1b-C08', 'ABI-L1b-C09', 'ABI-L1b-C10'] },
        { satellite: 'GOES-19', region: 'M1', channels: ['ABI-L1b-C13', 'ABI-L1b-C14', 'ABI-L1b-C03', 'ABI-L1b-C08', 'ABI-L1b-C09', 'ABI-L1b-C10'] },
        { satellite: 'GOES-19', region: 'M2', channels: ['ABI-L1b-C13', 'ABI-L1b-C14', 'ABI-L1b-C03', 'ABI-L1b-C08', 'ABI-L1b-C09', 'ABI-L1b-C10'] },
        // GOES-18 (West)
        { satellite: 'GOES-18', region: 'CONUS', channels: ['ABI-L1b-C13', 'ABI-L1b-C14', 'ABI-L1b-C03', 'ABI-L1b-C08', 'ABI-L1b-C09', 'ABI-L1b-C10'] },
        { satellite: 'GOES-18', region: 'M1', channels: ['ABI-L1b-C13', 'ABI-L1b-C14', 'ABI-L1b-C03', 'ABI-L1b-C08', 'ABI-L1b-C09', 'ABI-L1b-C10'] },
        { satellite: 'GOES-18', region: 'M2', channels: ['ABI-L1b-C13', 'ABI-L1b-C14', 'ABI-L1b-C03', 'ABI-L1b-C08', 'ABI-L1b-C09', 'ABI-L1b-C10'] }
    ],
    composite: [
        { country: 'USA', product: 'MRMS', region: 'CONUS', types: ['MergedBaseReflectivityQC', 'MergedBaseReflectivity', 'PrecipRate'] }
    ]
};

function connectToWeatherwise() {
    console.log('🔌 Connecting to weatherwise WebSocket...');
    
    weatherwiseWS = new WebSocket(WEATHERWISE_WS_URL);
    
    weatherwiseWS.on('open', () => {
        console.log('✅ Connected to weatherwise WebSocket');
        subscribeToAllProducts();
    });
    
    weatherwiseWS.on('message', (data) => {
        try {
            const message = JSON.parse(data);
            console.log('📨 Received from weatherwise:', message.type || 'unknown');
            
            // Convert .wise to .plots in the message
            const convertedMessage = convertWiseToPlots(message);
            
            // Broadcast to all connected clients
            const messageStr = JSON.stringify(convertedMessage);
            connectedClients.forEach(client => {
                if (client.readyState === WebSocket.OPEN) {
                    client.send(messageStr);
                }
            });
        } catch (error) {
            console.error('Error processing weatherwise message:', error);
        }
    });
    
    weatherwiseWS.on('close', () => {
        console.log('❌ Disconnected from weatherwise WebSocket');
        // Reconnect after 5 seconds
        setTimeout(connectToWeatherwise, 5000);
    });
    
    weatherwiseWS.on('error', (error) => {
        console.error('🚨 Weatherwise WebSocket error:', error);
    });
}

function subscribeToAllProducts() {
    console.log('📡 Subscribing to all supported products...');
    
    // Subscribe to satellite products
    SUPPORTED_PRODUCTS.satellite.forEach(config => {
        config.channels.forEach(channel => {
            const subscription = {
                type: 'subscribe',
                product: 'satellite',
                satellite: config.satellite,
                region: config.region,
                channel: channel
            };
            weatherwiseWS.send(JSON.stringify(subscription));
            console.log(`🛰️  Subscribed to ${config.satellite}/${config.region}/${channel}`);
        });
    });
    
    // Subscribe to composite products
    SUPPORTED_PRODUCTS.composite.forEach(config => {
        config.types.forEach(type => {
            const subscription = {
                type: 'subscribe',
                product: 'composite',
                country: config.country,
                productType: config.product,
                region: config.region,
                dataType: type
            };
            weatherwiseWS.send(JSON.stringify(subscription));
            console.log(`🌧️  Subscribed to ${config.country}/${config.product}/${config.region}/${type}`);
        });
    });
}

function convertWiseToPlots(message) {
    // Convert any .wise file references to .plots
    const messageStr = JSON.stringify(message);
    const convertedStr = messageStr.replace(/\.wise/g, '.plots');
    return JSON.parse(convertedStr);
}

// Helper function for HTTP fallback
async function fetchFromWeatherwise(endpoint) {
    for (const baseUrl of WEATHERWISE_HTTP_ENDPOINTS) {
        try {
            const url = `${baseUrl}${endpoint}`;
            console.log(`Trying to fetch from: ${url}`);
            
            const response = await fetch(url, {
                timeout: 15000,
                headers: {
                    'User-Agent': 'PettusPlots-DataProxy/1.0'
                }
            });
            
            if (response.ok) {
                return response;
            }
        } catch (error) {
            console.log(`Failed to fetch from ${baseUrl}: ${error.message}`);
            continue;
        }
    }
    throw new Error('All weatherwise endpoints failed');
}

// Satellite data proxy - converts .wise to .plots
app.get('/satellite/:satellite/:region/:channel/:filename', async (req, res) => {
    try {
        const { satellite, region, channel, filename } = req.params;
        
        // Convert .plots extension back to .wise for weatherwise API
        const wiseFilename = filename.replace('.plots', '.wise');
        const weatherwiseEndpoint = `/satellite/${satellite}/${region}/${channel}/${wiseFilename}`;
        
        console.log(`Proxying satellite request: ${filename} -> ${wiseFilename}`);
        
        const response = await fetchFromWeatherwise(weatherwiseEndpoint);
        
        // Set appropriate headers
        res.set({
            'Content-Type': response.headers.get('content-type') || 'application/octet-stream',
            'Content-Length': response.headers.get('content-length'),
            'Cache-Control': 'public, max-age=300' // 5 minutes cache
        });
        
        // Stream the binary data
        response.body.pipe(res);
        
    } catch (error) {
        console.error(`Error proxying satellite data: ${error.message}`);
        res.status(500).json({ 
            error: 'Failed to fetch satellite data',
            message: error.message 
        });
    }
});

// Directory listing proxy - converts .wise to .plots in file lists
app.get('/satellite/:satellite/:region/:channel/dir.list', async (req, res) => {
    try {
        const { satellite, region, channel } = req.params;
        const weatherwiseEndpoint = `/satellite/${satellite}/${region}/${channel}/dir.list`;
        
        console.log(`Proxying directory listing: ${weatherwiseEndpoint}`);
        
        const response = await fetchFromWeatherwise(weatherwiseEndpoint);
        const text = await response.text();
        
        // Convert all .wise extensions to .plots in the directory listing
        const convertedText = text.replace(/\.wise/g, '.plots');
        
        res.set({
            'Content-Type': 'text/plain',
            'Cache-Control': 'public, max-age=60' // 1 minute cache for listings
        });
        
        res.send(convertedText);
        
    } catch (error) {
        console.error(`Error proxying directory listing: ${error.message}`);
        res.status(500).json({ 
            error: 'Failed to fetch directory listing',
            message: error.message 
        });
    }
});

// Composite data proxy (if needed) - converts .wise to .plots
app.get('/composites/processed/:country/:product/:region/:productType/:filename', async (req, res) => {
    try {
        const { country, product, region, productType, filename } = req.params;
        
        // Convert .plots extension back to .wise for weatherwise API
        const wiseFilename = filename.replace('.plots', '.wise');
        const weatherwiseEndpoint = `/composites/processed/${country}/${product}/${region}/${productType}/${wiseFilename}`;
        
        console.log(`Proxying composite request: ${filename} -> ${wiseFilename}`);
        
        const response = await fetchFromWeatherwise(weatherwiseEndpoint);
        
        // Set appropriate headers
        res.set({
            'Content-Type': response.headers.get('content-type') || 'application/octet-stream',
            'Content-Length': response.headers.get('content-length'),
            'Cache-Control': 'public, max-age=300' // 5 minutes cache
        });
        
        // Stream the binary data
        response.body.pipe(res);
        
    } catch (error) {
        console.error(`Error proxying composite data: ${error.message}`);
        res.status(500).json({ 
            error: 'Failed to fetch composite data',
            message: error.message 
        });
    }
});

// Composite directory listing proxy
app.get('/composites/processed/:country/:product/:region/:productType/dir.list', async (req, res) => {
    try {
        const { country, product, region, productType } = req.params;
        const weatherwiseEndpoint = `/composites/processed/${country}/${product}/${region}/${productType}/dir.list`;
        
        console.log(`Proxying composite directory listing: ${weatherwiseEndpoint}`);
        
        const response = await fetchFromWeatherwise(weatherwiseEndpoint);
        const text = await response.text();
        
        // Convert all .wise extensions to .plots in the directory listing
        const convertedText = text.replace(/\.wise/g, '.plots');
        
        res.set({
            'Content-Type': 'text/plain',
            'Cache-Control': 'public, max-age=60' // 1 minute cache for listings
        });
        
        res.send(convertedText);
        
    } catch (error) {
        console.error(`Error proxying composite directory listing: ${error.message}`);
        res.status(500).json({ 
            error: 'Failed to fetch composite directory listing',
            message: error.message 
        });
    }
});

// Radar data proxy (if needed)
app.get('/radar/:site/:product/:filename', async (req, res) => {
    try {
        const { site, product, filename } = req.params;
        
        // Convert .plots extension back to .wise for weatherwise API
        const wiseFilename = filename.replace('.plots', '.wise');
        const weatherwiseEndpoint = `/radar/${site}/${product}/${wiseFilename}`;
        
        console.log(`Proxying radar request: ${filename} -> ${wiseFilename}`);
        
        const response = await fetchFromWeatherwise(weatherwiseEndpoint);
        
        // Set appropriate headers
        res.set({
            'Content-Type': response.headers.get('content-type') || 'application/octet-stream',
            'Content-Length': response.headers.get('content-length'),
            'Cache-Control': 'public, max-age=300' // 5 minutes cache
        });
        
        // Stream the binary data
        response.body.pipe(res);
        
    } catch (error) {
        console.error(`Error proxying radar data: ${error.message}`);
        res.status(500).json({ 
            error: 'Failed to fetch radar data',
            message: error.message 
        });
    }
});

// Health check endpoint
app.get('/health', (req, res) => {
    res.json({ 
        status: 'ok', 
        service: 'PettusPlots Data Proxy',
        timestamp: new Date().toISOString(),
        uptime: process.uptime()
    });
});

// Root endpoint
app.get('/', (req, res) => {
    res.json({
        service: 'PettusPlots Data Proxy Server',
        description: 'Converts weatherwise .wise files to .plots format',
        version: '1.0.0',
        endpoints: {
            satellite: '/satellite/:satellite/:region/:channel/:filename',
            satelliteList: '/satellite/:satellite/:region/:channel/dir.list',
            composite: '/composites/processed/:country/:product/:region/:productType/:filename',
            compositeList: '/composites/processed/:country/:product/:region/:productType/dir.list',
            radar: '/radar/:site/:product/:filename',
            health: '/health'
        }
    });
});

// Error handling middleware
app.use((err, req, res, next) => {
    console.error('Server error:', err);
    res.status(500).json({
        error: 'Internal server error',
        message: err.message
    });
});

// WebSocket server for client connections
wss.on('connection', (ws, req) => {
    console.log('🔗 New client connected');
    connectedClients.add(ws);
    
    // Send welcome message
    ws.send(JSON.stringify({
        type: 'connected',
        message: 'Connected to PettusPlots Data Proxy',
        timestamp: new Date().toISOString()
    }));
    
    ws.on('close', () => {
        console.log('❌ Client disconnected');
        connectedClients.delete(ws);
    });
    
    ws.on('error', (error) => {
        console.error('🚨 Client WebSocket error:', error);
        connectedClients.delete(ws);
    });
    
    // Handle client subscription requests
    ws.on('message', (data) => {
        try {
            const message = JSON.parse(data);
            console.log('📨 Client message:', message);
            
            // Forward subscription requests to weatherwise
            if (weatherwiseWS && weatherwiseWS.readyState === WebSocket.OPEN) {
                weatherwiseWS.send(data);
            }
        } catch (error) {
            console.error('Error processing client message:', error);
        }
    });
});

// Start server and connect to weatherwise
server.listen(PORT, () => {
    console.log(`🚀 PettusPlots Data Proxy Server running on port ${PORT}`);
    console.log(`📡 WebSocket server available at ws://localhost:${PORT}`);
    console.log(`🌐 Health check available at http://localhost:${PORT}/health`);
    console.log(`🔄 Proxying data from weatherwise with .plots branding`);
    
    // Connect to weatherwise WebSocket
    connectToWeatherwise();
});

// Graceful shutdown
process.on('SIGTERM', () => {
    console.log('🛑 Shutting down server...');
    
    if (weatherwiseWS) {
        weatherwiseWS.close();
    }
    
    connectedClients.forEach(client => {
        client.close();
    });
    
    server.close(() => {
        console.log('✅ Server shut down gracefully');
        process.exit(0);
    });
});

module.exports = app;
