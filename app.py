from flask import Flask, jsonify, send_file, request
import os
import time
import threading
import schedule
from datetime import datetime, timedelta
import sqlite3
from pathlib import Path
import json
import logging
from contextlib import contextmanager
import shutil
import matplotlib.pyplot as plt
import numpy as np

app = Flask(__name__)

# Configuration
MAX_IMAGES = 500
IMAGE_DIR = "images"
DB_FILE = "image_metadata.db"
GENERATION_INTERVAL = 15  # minutes between image generations
DOMAIN = "data.pettusplots.com"

# Ensure directories exist
os.makedirs(IMAGE_DIR, exist_ok=True)

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# For now, we'll use mock images until you add your GOES script
GOES_AVAILABLE = False

class ImageManager:
    def __init__(self):
        self.init_database()
        self.cleanup_on_startup()
        
    def init_database(self):
        """Initialize SQLite database for image metadata"""
        with sqlite3.connect(DB_FILE) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS images (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    filename TEXT UNIQUE NOT NULL,
                    filepath TEXT NOT NULL,
                    timestamp DATETIME NOT NULL,
                    satellite TEXT,
                    sector TEXT,
                    product TEXT,
                    band TEXT,
                    url_path TEXT UNIQUE NOT NULL,
                    custom_text TEXT,
                    file_size INTEGER,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            conn.commit()
    
    @contextmanager
    def get_db(self):
        """Context manager for database connections"""
        conn = sqlite3.connect(DB_FILE)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()
    
    def cleanup_on_startup(self):
        """Clean up orphaned files and database entries on startup"""
        logger.info("Performing startup cleanup...")
        
        with self.get_db() as conn:
            # Get all database entries
            db_files = set()
            for row in conn.execute("SELECT filepath FROM images"):
                db_files.add(row['filepath'])
            
            # Get all actual files
            actual_files = set()
            if os.path.exists(IMAGE_DIR):
                for file in os.listdir(IMAGE_DIR):
                    if file.endswith('.png'):
                        actual_files.add(os.path.join(IMAGE_DIR, file))
            
            # Remove database entries for missing files
            missing_files = db_files - actual_files
            for filepath in missing_files:
                conn.execute("DELETE FROM images WHERE filepath = ?", (filepath,))
                logger.info(f"Removed database entry for missing file: {filepath}")
            
            # Remove orphaned files not in database
            orphaned_files = actual_files - db_files
            for filepath in orphaned_files:
                try:
                    os.remove(filepath)
                    logger.info(f"Removed orphaned file: {filepath}")
                except Exception as e:
                    logger.error(f"Error removing orphaned file {filepath}: {e}")
            
            conn.commit()
        
        logger.info("Startup cleanup completed")
    
    def generate_descriptive_url(self, timestamp, satellite, sector, product, band):
        """Generate descriptive URL path with sector, time, and product info"""
        # Format: /goes/GOES19_FullDisk_Band13_CleanIR_20231201_1430Z
        
        # Clean satellite name
        sat_clean = satellite.replace('noaa-', '').replace('-', '').upper()
        
        # Format timestamp
        time_str = timestamp.strftime('%Y%m%d_%H%MZ')
        
        # Sector mapping
        sector_map = {
            'F': 'FullDisk',
            'C': 'CONUS', 
            'M': 'Mesoscale'
        }
        sector_name = sector_map.get(sector, sector)
        
        # Product mapping
        product_map = {
            'ABI-L2-MCMIPF': 'Multichannel',
            'ABI-L2-MCMIPC': 'Multichannel',
            'ABI-L2-MCMIPM': 'Multichannel',
            'ABI-L1b-Rad': 'Radiance',
            'MOCK': 'MockData'
        }
        product_name = product_map.get(product, product)
        
        # Band info
        band_info = f"Band{band}_CleanIR" if band == "13" else f"Band{band}"
        
        # Construct URL path
        url_path = f"/goes/{sat_clean}_{sector_name}_{band_info}_{product_name}_{time_str}"
        
        return url_path
    
    def generate_mock_image(self, custom_text=None):
        """Generate a mock GOES-style image for testing"""
        logger.info("Generating mock GOES image...")
        
        # Create a realistic-looking satellite image
        fig, ax = plt.subplots(figsize=(16, 10), facecolor='white')
        
        # Generate realistic-looking satellite data with temperature patterns
        np.random.seed(int(time.time()) % 1000)  # Semi-random but reproducible
        
        # Create temperature-like data (simulating infrared imagery)
        x = np.linspace(-50, 50, 200)
        y = np.linspace(-30, 30, 150)
        X, Y = np.meshgrid(x, y)
        
        # Simulate cloud patterns and temperature gradients
        temp_data = (
            230 +  # Base temperature
            20 * np.sin(X/10) * np.cos(Y/8) +  # Large-scale patterns
            15 * np.random.random((150, 200)) +  # Random variation
            -30 * np.exp(-((X-10)**2 + (Y-5)**2)/100) +  # Cold cloud system
            -25 * np.exp(-((X+15)**2 + (Y+10)**2)/150)   # Another cold area
        )
        
        # Add some noise for realism
        temp_data += np.random.normal(0, 3, temp_data.shape)
        
        # Plot with infrared-style colormap
        im = ax.imshow(temp_data, extent=[-50, 50, -30, 30], 
                      cmap='gray_r', vmin=180, vmax=300, aspect='auto')
        
        # Add map-like features
        ax.set_xlim(-50, 50)
        ax.set_ylim(-30, 30)
        ax.set_xlabel('Longitude', fontsize=14)
        ax.set_ylabel('Latitude', fontsize=14)
        
        # Title and timestamp
        timestamp_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')
        ax.set_title(f'Mock GOES-19 Band 13 (Clean IR) - {timestamp_str}', 
                    fontsize=18, fontweight='bold', pad=20)
        
        # Add custom text if provided
        if custom_text:
            ax.text(0.98, 0.95, custom_text, transform=ax.transAxes, 
                   fontsize=16, ha='right', va='top', 
                   bbox=dict(boxstyle='round,pad=0.5', facecolor='white', alpha=0.8),
                   color='darkblue', fontweight='bold')
        
        # Add colorbar
        cbar = plt.colorbar(im, ax=ax, shrink=0.8, aspect=20)
        cbar.set_label('Brightness Temperature (K)', fontsize=12)
        
        # Add some grid lines for reference
        ax.grid(True, alpha=0.3, linestyle='--')
        
        # Generate metadata
        timestamp = datetime.now()
        satellite = "GOES-19"
        sector = "F"
        product = "MOCK"
        band = "13"
        
        # Generate descriptive URL
        url_path = self.generate_descriptive_url(timestamp, satellite, sector, product, band)
        
        # Create filename
        filename = url_path.replace('/goes/', '') + '.png'
        filepath = os.path.join(IMAGE_DIR, filename)
        
        # Save with high quality
        plt.savefig(filepath, dpi=200, bbox_inches='tight', 
                   facecolor='white', edgecolor='none')
        plt.close(fig)
        
        # Store metadata
        file_size = os.path.getsize(filepath)
        with self.get_db() as conn:
            conn.execute('''
                INSERT INTO images (filename, filepath, timestamp, satellite,
                                  sector, product, band, url_path, custom_text, file_size)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (filename, filepath, timestamp, satellite, sector, product,
                  band, url_path, custom_text or "", file_size))
            conn.commit()
        
        logger.info(f"Mock image generated: {filename}")
        logger.info(f"URL: https://{DOMAIN}{url_path}")
        
        self.cleanup_old_images()
        return filepath, url_path
    
    def generate_image(self, custom_text=None):
        """Generate a new image (mock for now, GOES later)"""
        try:
            # For now, always generate mock images
            # Later you can add: if GOES_AVAILABLE: create_professional_band13_plot()
            return self.generate_mock_image(custom_text)
        except Exception as e:
            logger.error(f"Error generating image: {e}")
            return None, None
    
    def cleanup_old_images(self):
        """Remove old images when we exceed MAX_IMAGES"""
        with self.get_db() as conn:
            # Count current images
            count_result = conn.execute("SELECT COUNT(*) as count FROM images").fetchone()
            current_count = count_result['count']
            
            if current_count > MAX_IMAGES:
                # Get oldest images to delete
                excess_count = current_count - MAX_IMAGES
                old_images = conn.execute('''
                    SELECT id, filepath FROM images 
                    ORDER BY timestamp ASC 
                    LIMIT ?
                ''', (excess_count,)).fetchall()
                
                # Delete files and database entries
                for image in old_images:
                    try:
                        if os.path.exists(image['filepath']):
                            os.remove(image['filepath'])
                        conn.execute("DELETE FROM images WHERE id = ?", (image['id'],))
                        logger.info(f"Removed old image: {image['filepath']}")
                    except Exception as e:
                        logger.error(f"Error removing old image {image['filepath']}: {e}")
                
                conn.commit()
                logger.info(f"Cleaned up {excess_count} old images")
    
    def get_all_images(self):
        """Get metadata for all stored images"""
        with self.get_db() as conn:
            images = conn.execute('''
                SELECT id, filename, timestamp, satellite, sector, product, band,
                       url_path, custom_text, file_size, created_at
                FROM images 
                ORDER BY timestamp DESC
            ''').fetchall()
            
            return [dict(row) for row in images]
    
    def get_image_by_url_path(self, url_path):
        """Get image metadata by URL path"""
        with self.get_db() as conn:
            image = conn.execute('''
                SELECT * FROM images WHERE url_path = ?
            ''', (url_path,)).fetchone()
            
            return dict(image) if image else None
    
    def get_latest_image(self):
        """Get the most recent image"""
        with self.get_db() as conn:
            image = conn.execute('''
                SELECT * FROM images 
                ORDER BY timestamp DESC 
                LIMIT 1
            ''').fetchone()
            
            return dict(image) if image else None

# Initialize the image manager
image_manager = ImageManager()

# Scheduler for automatic image generation
def scheduled_image_generation():
    """Generate images on schedule"""
    logger.info("Scheduled image generation triggered")
    timestamp = datetime.now().strftime('%H:%M UTC')
    custom_text = f"Auto {timestamp}"
    image_manager.generate_image(custom_text=custom_text)

# Schedule automatic generation
schedule.every(GENERATION_INTERVAL).minutes.do(scheduled_image_generation)

def run_scheduler():
    """Run the scheduler in a separate thread"""
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute

# Start scheduler thread
scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
scheduler_thread.start()

# API Routes
@app.route('/health')
def health_check():
    """Health check endpoint"""
    try:
        # Check database connectivity
        with image_manager.get_db() as conn:
            conn.execute("SELECT 1").fetchone()
        
        # Get latest image info
        latest = image_manager.get_latest_image()
        
        return jsonify({
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "goes_available": GOES_AVAILABLE,
            "total_images": len(image_manager.get_all_images()),
            "latest_image": latest['url_path'] if latest else None,
            "domain": DOMAIN,
            "mode": "mock" if not GOES_AVAILABLE else "real"
        })
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500

@app.route('/goes')
def list_goes_images():
    """List all GOES images with their descriptive URLs"""
    try:
        images = image_manager.get_all_images()
        
        # Format for easy consumption
        formatted_images = []
        for img in images:
            formatted_images.append({
                "url": f"https://{DOMAIN}{img['url_path']}",
                "path": img['url_path'],
                "satellite": img['satellite'],
                "sector": img['sector'],
                "product": img['product'],
                "band": img['band'],
                "timestamp": img['timestamp'],
                "custom_text": img['custom_text'],
                "size_kb": round(img['file_size'] / 1024, 1) if img['file_size'] else 0
            })
        
        return jsonify({
            "success": True,
            "count": len(formatted_images),
            "domain": DOMAIN,
            "mode": "mock" if not GOES_AVAILABLE else "real",
            "images": formatted_images,
            "latest": formatted_images[0] if formatted_images else None
        })
        
    except Exception as e:
        logger.error(f"Error listing GOES images: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/goes/<path:image_path>')
def serve_goes_image(image_path):
    """Serve GOES images by their descriptive URL path"""
    try:
        # Reconstruct the full URL path
        url_path = f"/goes/{image_path}"
        
        # Remove .png extension if present in the URL
        if url_path.endswith('.png'):
            url_path = url_path[:-4]
        
        # Find the image in database
        image_data = image_manager.get_image_by_url_path(url_path)
        
        if image_data and os.path.exists(image_data['filepath']):
            # Log access
            logger.info(f"Serving image: {url_path}")
            
            return send_file(
                image_data['filepath'],
                as_attachment=False,
                download_name=image_data['filename'],
                mimetype='image/png'
            )
        else:
            # If not found, try to generate a new image
            logger.info(f"Image not found: {url_path}, generating new image...")
            
            filepath, new_url_path = image_manager.generate_image(
                custom_text=f"On-demand {datetime.now().strftime('%H:%M')}"
            )
            
            if filepath and new_url_path:
                # Return the newly generated image
                return send_file(
                    filepath,
                    as_attachment=False,
                    download_name=os.path.basename(filepath),
                    mimetype='image/png'
                )
            else:
                return jsonify({
                    "success": False,
                    "error": "Image not found and could not generate new image",
                    "requested_path": url_path
                }), 404
            
    except Exception as e:
        logger.error(f"Error serving GOES image {image_path}: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

# Generate new image endpoint (for manual triggering)
@app.route('/goes/generate', methods=['POST'])
def generate_new_image():
    """Generate a new GOES image (manual trigger)"""
    try:
        data = request.get_json() or {}
        custom_text = data.get('custom_text', f"Manual {datetime.now().strftime('%H:%M')}")
        
        logger.info(f"Manual image generation requested with text: '{custom_text}'")
        
        filepath, url_path = image_manager.generate_image(custom_text=custom_text)
        
        if filepath and url_path:
            return jsonify({
                "success": True,
                "message": "Image generated successfully",
                "url": f"https://{DOMAIN}{url_path}",
                "path": url_path,
                "mode": "mock" if not GOES_AVAILABLE else "real"
            })
        else:
            return jsonify({
                "success": False,
                "error": "Failed to generate image"
            }), 500
            
    except Exception as e:
        logger.error(f"Error generating image: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

# Root redirect
@app.route('/')
def root():
    """Root endpoint - redirect to /goes"""
    return jsonify({
        "message": "GOES Satellite Data Server",
        "domain": DOMAIN,
        "mode": "mock" if not GOES_AVAILABLE else "real",
        "endpoints": {
            "/goes": "List all GOES images",
            "/goes/<descriptive_path>": "Access specific GOES image",
            "/health": "Health check"
        },
        "example_urls": [
            f"https://{DOMAIN}/goes",
            f"https://{DOMAIN}/goes/GOES19_FullDisk_Band13_CleanIR_MockData_20231201_1430Z",
            f"https://{DOMAIN}/health"
        ]
    })

# Error handlers
@app.errorhandler(404)
def not_found(error):
    return jsonify({
        "success": False, 
        "error": "Endpoint not found",
        "available_endpoints": ["/goes", "/health"]
    }), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({"success": False, "error": "Internal server error"}), 500

if __name__ == '__main__':
    # Generate an initial image if none exist
    if not image_manager.get_latest_image():
        logger.info("No images found, generating initial image...")
        image_manager.generate_image(custom_text="Server Started")
    
    # Run the Flask app
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
