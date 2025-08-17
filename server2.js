const express = require('express');
const http = require('http');
const fetch = require('node-fetch');

const app = express();
const server = http.createServer(app);
const PORT = process.env.PORT || 8080;

// Enable trust proxy for Railway
app.set('trust proxy', true);

// Add CORS headers for cross-origin requests
app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
    res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, Authorization');
    
    if (req.method === 'OPTIONS') {
        res.sendStatus(200);
    } else {
        next();
    }
});

// Request logging middleware
app.use((req, res, next) => {
    console.log(`${new Date().toISOString()} - ${req.method} ${req.path} - ${req.ip}`);
    next();
});

// Weatherwise HTTP endpoints
const WEATHERWISE_HTTP_ENDPOINTS = [
    'https://data2.weatherwise.app',
    'https://data1.weatherwise.app'
];

// Satellite configuration
const SATELLITES = ['GOES-18', 'GOES-19'];
const SECTORS = ['CONUS', 'M1', 'M2'];
const BANDS = ['2', '7', '8', '9', '10', '13'];

// Cache for storing latest frames
const frameCache = new Map();
const M_SECTOR_CACHE_DURATION = 50 * 1000; // 50 seconds for M1, M2
const CONUS_CACHE_DURATION = 4 * 60 * 1000; // 4 minutes for CONUS

// Helper function for HTTP requests
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

// Fetch directory listing and get last 50 frames
async function fetchLatestFrames(satellite, sector, band) {
    const cacheKey = `${satellite}-${sector}-${band}`;
    const cached = frameCache.get(cacheKey);
    
    // Determine cache duration based on sector type
    const cacheDuration = (sector === 'M1' || sector === 'M2') ? M_SECTOR_CACHE_DURATION : CONUS_CACHE_DURATION;
    
    // Return cached data if still valid
    if (cached && (Date.now() - cached.timestamp) < cacheDuration) {
        return cached.frames;
    }
    
    try {
        const dirUrl = `/satellite/processed/${satellite}/${sector}/ABI-L1b-C${band}/dir.list`;
        console.log(`📡 Fetching directory listing: ${dirUrl}`);
        
        const response = await fetchFromWeatherwise(dirUrl);
        const text = await response.text();
        
        // Parse directory listing to get .wise files
        const files = text.split('\n')
            .filter(line => line.trim() && line.endsWith('.wise'))
            .map(line => line.trim())
            .sort()
            .slice(-50); // Get last 50 files
        
        // Convert .wise to .plots
        const frames = files.map(file => file.replace('.wise', '.plots'));
        
        // Cache the results
        frameCache.set(cacheKey, {
            frames,
            timestamp: Date.now()
        });
        
        console.log(`✅ Cached ${frames.length} frames for ${satellite}/${sector}/C${band}`);
        return frames;
        
    } catch (error) {
        console.error(`❌ Error fetching frames for ${satellite}/${sector}/C${band}:`, error.message);
        return [];
    }
}

// Update cache for all products
async function updateAllFrames() {
    console.log('🔄 Updating frame cache for all satellite products...');
    
    const updatePromises = [];
    
    for (const satellite of SATELLITES) {
        for (const sector of SECTORS) {
            for (const band of BANDS) {
                updatePromises.push(fetchLatestFrames(satellite, sector, band));
            }
        }
    }
    
    try {
        await Promise.all(updatePromises);
        console.log('✅ Frame cache updated successfully');
    } catch (error) {
        console.error('❌ Error updating frame cache:', error);
    }
}

// Update cache for M sectors (every 50 seconds)
async function updateMSectorFrames() {
    console.log('🔄 Updating M sector frame cache...');
    
    const updatePromises = [];
    
    for (const satellite of SATELLITES) {
        for (const sector of ['M1', 'M2']) {
            for (const band of BANDS) {
                updatePromises.push(fetchLatestFrames(satellite, sector, band));
            }
        }
    }
    
    try {
        await Promise.all(updatePromises);
        console.log('✅ M sector frame cache updated');
    } catch (error) {
        console.error('❌ Error updating M sector frame cache:', error);
    }
}

// Update cache for CONUS sector (every 4 minutes)
async function updateCONUSFrames() {
    console.log('🔄 Updating CONUS frame cache...');
    
    const updatePromises = [];
    
    for (const satellite of SATELLITES) {
        for (const band of BANDS) {
            updatePromises.push(fetchLatestFrames(satellite, 'CONUS', band));
        }
    }
    
    try {
        await Promise.all(updatePromises);
        console.log('✅ CONUS frame cache updated');
    } catch (error) {
        console.error('❌ Error updating CONUS frame cache:', error);
    }
}

// Start periodic cache updates with different intervals
function startCacheUpdates() {
    // Update immediately
    updateAllFrames();
    
    // Update M sectors every 50 seconds
    setInterval(updateMSectorFrames, 50 * 1000);
    console.log('⏰ Started M sector cache updates (every 50 seconds)');
    
    // Update CONUS every 4 minutes
    setInterval(updateCONUSFrames, 4 * 60 * 1000);
    console.log('⏰ Started CONUS cache updates (every 4 minutes)');
}

// Main GOES live endpoint - Get all satellite products with .plots file links
app.get('/goes/live', async (req, res) => {
    try {
        const result = {};
        
        for (const satellite of SATELLITES) {
            result[satellite] = {};
            for (const sector of SECTORS) {
                result[satellite][sector] = {};
                for (const band of BANDS) {
                    const frames = await fetchLatestFrames(satellite, sector, band);
                    // Convert to full URLs with .plots extension
                    const frameUrls = frames.map(frame => 
                        `https://data.pettusplots.online/satellite/${satellite}/${sector}/ABI-L1b-C${band}/${frame}`
                    );
                    result[satellite][sector][`C${band}`] = frameUrls;
                }
            }
        }
        
        res.json({
            success: true,
            timestamp: new Date().toISOString(),
            goes: result
        });
        
    } catch (error) {
        console.error('Error generating GOES live API:', error);
        res.status(500).json({
            success: false,
            error: 'Failed to fetch latest frames',
            message: error.message
        });
    }
});

// Legacy API endpoint - Get latest frames for all products
app.get('/api/latest-frames', async (req, res) => {
    try {
        const result = {};
        
        for (const satellite of SATELLITES) {
            result[satellite] = {};
            for (const sector of SECTORS) {
                result[satellite][sector] = {};
                for (const band of BANDS) {
                    const frames = await fetchLatestFrames(satellite, sector, band);
                    const frameUrls = frames.map(frame => 
                        `https://data.pettusplots.online/satellite/${satellite}/${sector}/ABI-L1b-C${band}/${frame}`
                    );
                    result[satellite][sector][`C${band}`] = frameUrls;
                }
            }
        }
        
        res.json({
            success: true,
            timestamp: new Date().toISOString(),
            data: result
        });
        
    } catch (error) {
        console.error('Error generating latest frames API:', error);
        res.status(500).json({
            success: false,
            error: 'Failed to fetch latest frames',
            message: error.message
        });
    }
});

// Get latest frames for specific satellite/sector/band
app.get('/api/latest-frames/:satellite/:sector/:band', async (req, res) => {
    try {
        const { satellite, sector, band } = req.params;
        
        // Validate parameters
        if (!SATELLITES.includes(satellite.toUpperCase())) {
            return res.status(400).json({
                success: false,
                error: 'Invalid satellite',
                valid_satellites: SATELLITES
            });
        }
        
        if (!SECTORS.includes(sector.toUpperCase())) {
            return res.status(400).json({
                success: false,
                error: 'Invalid sector',
                valid_sectors: SECTORS
            });
        }
        
        if (!BANDS.includes(band)) {
            return res.status(400).json({
                success: false,
                error: 'Invalid band',
                valid_bands: BANDS
            });
        }
        
        const frames = await fetchLatestFrames(satellite.toUpperCase(), sector.toUpperCase(), band);
        const frameUrls = frames.map(frame => 
            `https://data.pettusplots.online/satellite/${satellite.toUpperCase()}/${sector.toUpperCase()}/ABI-L1b-C${band}/${frame}`
        );
        
        res.json({
            success: true,
            satellite: satellite.toUpperCase(),
            sector: sector.toUpperCase(),
            band: `C${band}`,
            timestamp: new Date().toISOString(),
            frames: frameUrls
        });
        
    } catch (error) {
        console.error(`Error fetching frames for ${req.params.satellite}/${req.params.sector}/C${req.params.band}:`, error);
        res.status(500).json({
            success: false,
            error: 'Failed to fetch frames',
            message: error.message
        });
    }
});

// Satellite data proxy - converts .wise to .plots
app.get('/satellite/:satellite/:region/:channel/:filename', async (req, res) => {
    try {
        const { satellite, region, channel, filename } = req.params;
        
        // Convert .plots extension back to .wise for weatherwise API
        const wiseFilename = filename.replace('.plots', '.wise');
        const weatherwiseEndpoint = `/satellite/processed/${satellite}/${region}/${channel}/${wiseFilename}`;
        
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
        const weatherwiseEndpoint = `/satellite/processed/${satellite}/${region}/${channel}/dir.list`;
        
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

// Health check endpoint
app.get('/health', (req, res) => {
    const cacheStats = {
        totalCacheEntries: frameCache.size,
        cacheKeys: Array.from(frameCache.keys())
    };
    
    res.json({ 
        status: 'ok', 
        service: 'PettusPlots Data Proxy',
        timestamp: new Date().toISOString(),
        uptime: process.uptime(),
        cache: cacheStats,
        environment: process.env.NODE_ENV || 'development'
    });
});

// Root endpoint
app.get('/', (req, res) => {
    res.json({
        service: 'PettusPlots Data Proxy Server',
        description: 'Converts weatherwise .wise files to .plots format with master API',
        version: '1.0.0',
        baseUrl: 'https://data.pettusplots.online',
        endpoints: {
            goesLive: '/goes/live',
            legacyAPI: '/api/latest-frames',
            specificFrames: '/api/latest-frames/:satellite/:sector/:band',
            satellite: '/satellite/:satellite/:region/:channel/:filename',
            satelliteList: '/satellite/:satellite/:region/:channel/dir.list',
            health: '/health'
        },
        configuration: {
            satellites: SATELLITES,
            sectors: SECTORS,
            bands: BANDS,
            cacheEntries: frameCache.size,
            msectorCacheDuration: `${M_SECTOR_CACHE_DURATION / 1000} seconds`,
            conusCacheDuration: `${CONUS_CACHE_DURATION / 60000} minutes`,
            msectorUpdateInterval: '50 seconds',
            conusUpdateInterval: '4 minutes'
        },
        examples: {
            goesLive: 'https://data.pettusplots.online/goes/live',
            specificFrames: 'https://data.pettusplots.online/api/latest-frames/GOES-19/CONUS/13',
            fileProxy: 'https://data.pettusplots.online/satellite/GOES-19/CONUS/ABI-L1b-C13/example_file.plots'
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

// Start server and begin frame caching
server.listen(PORT, '0.0.0.0', () => {
    console.log(`🚀 PettusPlots Data Proxy Server running on port ${PORT}`);
    console.log(`🌐 Health check: https://data.pettusplots.online/health`);
    console.log(`📡 GOES Live API: https://data.pettusplots.online/goes/live`);
    console.log(`🔄 Proxying weatherwise data with .plots branding`);
    console.log(`📊 Supported: ${SATELLITES.length} satellites, ${SECTORS.length} sectors, ${BANDS.length} bands`);
    
    // Start frame caching
    startCacheUpdates();
}).on('error', (err) => {
    console.error('❌ Server failed to start:', err);
    process.exit(1);
});

// Graceful shutdown
process.on('SIGTERM', () => {
    console.log('🛑 Shutting down server...');
    
    server.close(() => {
        console.log('✅ Server shut down gracefully');
        process.exit(0);
    });
});

module.exports = app;
