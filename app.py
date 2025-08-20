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
import hashlib

# Import your GOES plotting functions
try:
    from goes_plotter import create_professional_band13_plot, set_custom_text
    GOES_AVAILABLE = True
    print("✅ Real GOES plotter loaded successfully")
except ImportError:
    print("❌ GOES plotter not available. Using mock images.")
    GOES_AVAILABLE = False
    # Mock functions for fallback
    import matplotlib.pyplot as plt
    import numpy as np

app = Flask(__name__)

# Configuration
MAX_IMAGES = 500
IMAGE_DIR = "images"
DB_FILE = "image_metadata.db"
GENERATION_INTERVAL = 3  # 3 minutes between image generations
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
        self.last_data_hash = None  # Track if data has changed
        
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
                    data_hash TEXT,
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
        # Format: /api/goes/GOES19_FullDisk_Band13_CleanIR_20231201_1430Z
        
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
        url_path = f"/api/goes/{sat_clean}_{sector_name}_{band_info}_{product_name}_{time_str}"
        
        return url_path
    
    def calculate_data_hash(self, timestamp_str, satellite):
        """Calculate a hash to identify unique data frames"""
        # Round timestamp to nearest 3 minutes to group related data
        try:
            dt = datetime.strptime(timestamp_str, '%Y%m%d_%H%MZ')
            # Round to nearest 3 minutes
            minutes = (dt.minute // 3) * 3
            rounded_dt = dt.replace(minute=minutes, second=0, microsecond=0)
            hash_input = f"{satellite}_{rounded_dt.strftime('%Y%m%d_%H%M')}"
            return hashlib.md5(hash_input.encode()).hexdigest()
        except:
            # Fallback to timestamp-based hash
            hash_input = f"{satellite}_{timestamp_str}"
            return hashlib.md5(hash_input.encode()).hexdigest()
    
    def check_if_frame_exists(self, data_hash):
        """Check if we already have this data frame"""
        with self.get_db() as conn:
            result = conn.execute(
                "SELECT COUNT(*) as count FROM images WHERE data_hash = ?", 
                (data_hash,)
            ).fetchone()
            return result['count'] > 0
    
    def generate_real_goes_image(self, custom_text=None):
        """Generate a real GOES image using your script"""
        logger.info("Generating real GOES satellite image...")
        
        try:
            # Set custom text if provided
            if custom_text:
                set_custom_text(custom_text)
            
            # Generate the image using your existing function
            saved_path = create_professional_band13_plot()
            
            if saved_path and os.path.exists(saved_path):
                # Extract metadata from the saved file or use current time
                timestamp = datetime.now()
                satellite = "GOES-19"  # Primary satellite from your script
                sector = "F"  # Full Disk from your script
                product = "ABI-L2-MCMIPF"  # Multichannel from your script
                band = "13"  # Band 13 from your script
                
                # Calculate data hash for duplicate detection
                time_str = timestamp.strftime('%Y%m%d_%H%MZ')
                data_hash = self.calculate_data_hash(time_str, satellite)
                
                # Check if we already have this frame
                if self.check_if_frame_exists(data_hash):
                    logger.info(f"🔄 Frame already exists for {time_str}, skipping generation")
                    os.remove(saved_path)  # Clean up the duplicate
                    return None, None
                
                # Generate descriptive URL
                url_path = self.generate_descriptive_url(timestamp, satellite, sector, product, band)
                
                # Create filename based on URL path
                filename = url_path.replace('/api/goes/', '') + '.png'
                new_path = os.path.join(IMAGE_DIR, filename)
                
                # Move to our managed directory with new name
                shutil.move(saved_path, new_path)
                
                # Get file size
                file_size = os.path.getsize(new_path)
                
                # Store metadata in database
                with self.get_db() as conn:
                    conn.execute('''
                        INSERT INTO images (filename, filepath, timestamp, satellite,
                                          sector, product, band, url_path, custom_text, 
                                          file_size, data_hash)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (filename, new_path, timestamp, satellite, sector, product, 
                          band, url_path, custom_text or "", file_size, data_hash))
                    conn.commit()
                
                logger.info(f"✅ Real GOES image generated: {filename}")
                logger.info(f"🔗 URL: https://{DOMAIN}{url_path}")
                
                # Update last data hash
                self.last_data_hash = data_hash
                
                # Cleanup old images if we exceed the limit
                self.cleanup_old_images()
                
                return new_path, url_path
            else:
                logger.error("❌ Failed to generate real GOES image")
                return None, None
                
        except Exception as e:
            logger.error(f"❌ Error generating real GOES image: {e}")
            import traceback
            traceback.print_exc()
            return None, None
    
    def generate_mock_image(self, custom_text=None):
        """Generate a mock image for testing when GOES is not available"""
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
        satellite = "MOCK-GOES19"
        sector = "F"
        product = "MOCK"
        band = "13"
        
        # Calculate data hash for duplicate detection
        time_str = timestamp.strftime('%Y%m%d_%H%MZ')
        data_hash = self.calculate_data_hash(time_str, satellite)
        
        # Check if we already have this frame
        if self.check_if_frame_exists(data_hash):
            logger.info(f"🔄 Mock frame already exists for {time_str}, skipping generation")
            plt.close(fig)
            return None, None
        
        # Generate descriptive URL
        url_path = self.generate_descriptive_url(timestamp, satellite, sector, product, band)
        
        # Create filename
        filename = url_path.replace('/api/goes/', '') + '.png'
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
                                  sector, product, band, url_path, custom_text, 
                                  file_size, data_hash)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (filename, filepath, timestamp, satellite, sector, product,
                  band, url_path, custom_text or "", file_size, data_hash))
            conn.commit()
        
        logger.info(f"✅ Mock image generated: {filename}")
        logger.info(f"🔗 URL: https://{DOMAIN}{url_path}")
        
        self.cleanup_old_images()
        return filepath, url_path
    
    def generate_image(self, custom_text=None):
        """Generate a new image (real GOES or mock)"""
        try:
            if GOES_AVAILABLE:
                return self.generate_real_goes_image(custom_text)
            else:
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

# Scheduler for automatic image generation every 3 minutes
def scheduled_image_generation():
    """Generate images on schedule - every 3 minutes with duplicate prevention"""
    logger.info("🕐 Scheduled image generation triggered (3-minute interval)")
    timestamp = datetime.now().strftime('%H:%M UTC')
    custom_text = f"Auto {timestamp}"
    
    filepath, url_path = image_manager.generate_image(custom_text=custom_text)
    
    if filepath and url_path:
        logger.info(f"✅ New image generated successfully")
    else:
        logger.info(f"⏭️ No new image needed (duplicate frame or error)")

# Schedule automatic generation every 3 minutes
schedule.every(GENERATION_INTERVAL).minutes.do(scheduled_image_generation)

def run_scheduler():
    """Run the scheduler in a separate thread"""
    while True:
        schedule.run_pending()
        time.sleep(30)  # Check every 30 seconds

# Start scheduler thread
scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
scheduler_thread.start()

# API Routes - Updated to use /api/ prefix
@app.route('/api/health')
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
            "mode": "real_goes" if GOES_AVAILABLE else "mock",
            "generation_interval_minutes": GENERATION_INTERVAL
        })
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500

@app.route('/api/goes')
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
            "mode": "real_goes" if GOES_AVAILABLE else "mock",
            "generation_interval_minutes": GENERATION_INTERVAL,
            "images": formatted_images,
            "latest": formatted_images[0] if formatted_images else None
        })
        
    except Exception as e:
        logger.error(f"Error listing GOES images: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/goes/<path:image_path>')
def serve_goes_image(image_path):
    """Serve GOES images by their descriptive URL path"""
    try:
        # Reconstruct the full URL path
        url_path = f"/api/goes/{image_path}"
        
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
@app.route('/api/goes/generate', methods=['POST'])
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
                "mode": "real_goes" if GOES_AVAILABLE else "mock"
            })
        else:
            return jsonify({
                "success": False,
                "error": "Image already exists or generation failed (duplicate frame)",
                "note": "Check if this time frame was already generated"
            }), 200  # Return 200 since this is normal behavior
            
    except Exception as e:
        logger.error(f"Error generating image: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

# Root and non-API redirects
@app.route('/')
def root():
    """Root endpoint - show API info"""
    return jsonify({
        "message": "GOES Satellite Data Server",
        "domain": DOMAIN,
        "mode": "real_goes" if GOES_AVAILABLE else "mock",
        "generation_interval_minutes": GENERATION_INTERVAL,
        "endpoints": {
            "/api/goes": "List all GOES images",
            "/api/goes/<descriptive_path>": "Access specific GOES image",
            "/api/health": "Health check"
        },
        "example_urls": [
            f"https://{DOMAIN}/api/goes",
            f"https://{DOMAIN}/api/goes/GOES19_FullDisk_Band13_CleanIR_Multichannel_20250820_2130Z",
            f"https://{DOMAIN}/api/health"
        ]
    })

# Legacy redirects (redirect old URLs to new API structure)
@app.route('/goes')
def legacy_goes_redirect():
    """Redirect old /goes to /api/goes"""
    return jsonify({
        "message": "Endpoint moved",
        "new_url": f"https://{DOMAIN}/api/goes",
        "note": "Please use /api/goes for the current API"
    }), 301

@app.route('/health')
def legacy_health_redirect():
    """Redirect old /health to /api/health"""
    return jsonify({
        "message": "Endpoint moved", 
        "new_url": f"https://{DOMAIN}/api/health",
        "note": "Please use /api/health for the current API"
    }), 301

# Error handlers
@app.errorhandler(404)
def not_found(error):
    return jsonify({
        "success": False, 
        "error": "Endpoint not found",
        "available_endpoints": ["/api/goes", "/api/health"],
        "note": "All endpoints now use /api/ prefix"
    }), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({"success": False, "error": "Internal server error"}), 500

if __name__ == '__main__':
    # Generate an initial image if none exist
    if not image_manager.get_latest_image():
        logger.info("No images found, generating initial image...")
        image_manager.generate_image(custom_text="Server Started")
    
    logger.info(f"🚀 GOES Server starting...")
    logger.info(f"🛰️ Mode: {'Real GOES Data' if GOES_AVAILABLE else 'Mock Data'}")
    logger.info(f"⏱️ Generation interval: {GENERATION_INTERVAL} minutes")
    logger.info(f"📡 Domain: {DOMAIN}")
    logger.info(f"🔗 API Base: https://{DOMAIN}/api/")
    
    # Run the Flask app
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
