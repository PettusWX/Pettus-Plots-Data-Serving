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
import re

# Import your GOES plotting functions (assuming they're in goes_plotter.py)
try:
    from goes_plotter import create_professional_band13_plot, set_custom_text
    GOES_AVAILABLE = True
except ImportError:
    print("Warning: GOES plotter not available. Using mock images.")
    GOES_AVAILABLE = False

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
            'ABI-L1b-Rad': 'Radiance'
        }
        product_name = product_map.get(product, product)
        
        # Band info
        band_info = f"Band{band}_CleanIR" if band == "13" else f"Band{band}"
        
        # Construct URL path
        url_path = f"/goes/{sat_clean}_{sector_name}_{band_info}_{product_name}_{time_str}"
        
        return url_path
    
    def generate_image(self, custom_text=None):
        """Generate a new GOES satellite image"""
        logger.info("Generating new GOES satellite image...")
        
        try:
            if GOES_AVAILABLE:
                # Set custom text if provided
                if custom_text:
                    set_custom_text(custom_text)
                
                # Generate the image using your existing function
                saved_path = create_professional_band13_plot()
                
                if saved_path and os.path.exists(saved_path):
                    # Extract metadata from your script's globals or make reasonable assumptions
                    timestamp = datetime.now()
                    satellite = "GOES-19"  # Primary satellite from your script
                    sector = "F"  # Full Disk from your script
                    product = "ABI-L2-MCMIPF"  # Multichannel from your script
                    band = "13"  # Band 13 from your script
                    
                    # Generate descriptive URL
                    url_path = self.generate_descriptive_url(timestamp, satellite, sector, product, band)
                    
                    # Create filename based on URL path (remove /goes/ prefix)
                    filename = url_path.replace('/goes/', '') + '.png'
                    new_path = os.path.join(IMAGE_DIR, filename)
                    
                    # Move to our managed directory with new name
                    shutil.move(saved_path, new_path)
                    
                    # Get file size
                    file_size = os.path.getsize(new_path)
                    
                    # Store metadata in database
                    with self.get_db() as conn:
                        conn.execute('''
                            INSERT INTO images (filename, filepath, timestamp, satellite,
                                              sector, product, band, url_path, custom_text, file_size)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (filename, new_path, timestamp, satellite, sector, product, 
                              band, url_path, custom_text or "", file_size))
                        conn.commit()
                    
                    logger.info(f"Image generated successfully: {filename}")
                    logger.info(f"URL: https://{DOMAIN}{url_path}")
                    
                    # Cleanup old images if we exceed the limit
                    self.cleanup_old_images()
                    
                    return new_path, url_path
                else:
                    logger.error("Failed to generate GOES image")
                    return None, None
            else:
                # Mock image generation for testing
                return self.generate_mock_image(custom_text)
                
        except Exception as e:
            logger.error(f"Error generating image: {e}")
            return None, None
    
    def generate_mock_image(self, custom_text=None):
        """Generate a mock image for testing when GOES is not available"""
        import matplotlib.pyplot as plt
        import numpy as np
        
        # Create a simple mock satellite image
        fig, ax = plt.subplots(figsize=(12, 8))
        
        # Generate some random "satellite-like" data
        data = np.random.rand(100, 100) * 255
        
        im = ax.imshow(data, cmap='gray_r')
        ax.set_title(f"Mock GOES Image - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        ax.set_xlabel("Longitude")
        ax.set_ylabel("Latitude")
        
        if custom_text:
            ax.text(0.95, 0.95, custom_text, transform=ax.transAxes, 
                   fontsize=14, ha='right', va='top', 
                   bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
        
        # Generate metadata
        timestamp = datetime.now()
        satellite = "MOCK-GOES"
        sector = "F"
        product = "MOCK"
        band = "13"
        
        # Generate descriptive URL
        url_path = self.generate_descriptive_url(timestamp, satellite, sector, product, band)
        
        # Create filename
        filename = url_path.replace('/goes/', '') + '.png'
        filepath = os.path.join(IMAGE_DIR, filename)
        
        plt.savefig(filepath, dpi=150, bbox_inches='tight')
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
            "domain": DOMAIN
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
                "path": url_path
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
        "endpoints": {
            "/goes": "List all GOES images",
            "/goes/<descriptive_path>": "Access specific GOES image",
            "/health": "Health check"
        },
        "example_urls": [
            f"https://{DOMAIN}/goes",
            f"https://{DOMAIN}/goes/GOES19_FullDisk_Band13_CleanIR_Multichannel_20231201_1430Z",
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
