#!/usr/bin/env python3
"""
data.pettusplots.com GOES API Client Example
===========================================

Simple client for interacting with the GOES satellite image server
at data.pettusplots.com with descriptive URLs.
"""

import requests
import json
import time
from datetime import datetime

# Your Railway deployment URL
BASE_URL = "https://data.pettusplots.com"

class GoesApiClient:
    def __init__(self, base_url):
        self.base_url = base_url.rstrip('/')
        
    def health_check(self):
        """Check if server is healthy"""
        try:
            response = requests.get(f"{self.base_url}/health")
            return response.json()
        except Exception as e:
            print(f"Error checking health: {e}")
            return None
    
    def list_all_images(self):
        """Get list of all available GOES images"""
        try:
            response = requests.get(f"{self.base_url}/goes")
            return response.json()
        except Exception as e:
            print(f"Error listing images: {e}")
            return None
    
    def get_image_by_url(self, descriptive_url, save_path=None):
        """Download a GOES image by its descriptive URL"""
        try:
            # Handle both full URLs and path-only URLs
            if descriptive_url.startswith('http'):
                url = descriptive_url
            elif descriptive_url.startswith('/goes/'):
                url = f"{self.base_url}{descriptive_url}"
            else:
                url = f"{self.base_url}/goes/{descriptive_url}"
            
            response = requests.get(url)
            
            if response.status_code == 200:
                if save_path is None:
                    # Extract filename from URL
                    path_parts = descriptive_url.split('/')
                    save_path = f"{path_parts[-1]}.png"
                    
                with open(save_path, 'wb') as f:
                    f.write(response.content)
                print(f"Image saved to: {save_path}")
                return save_path
            else:
                print(f"Error downloading image: {response.status_code}")
                try:
                    error_info = response.json()
                    print(f"Error details: {error_info}")
                except:
                    pass
                return None
        except Exception as e:
            print(f"Error downloading image: {e}")
            return None
    
    def generate_new_image(self, custom_text=""):
        """Generate a new image with optional custom text"""
        try:
            data = {"custom_text": custom_text} if custom_text else {}
            response = requests.post(
                f"{self.base_url}/goes/generate",
                json=data,
                headers={"Content-Type": "application/json"}
            )
            return response.json()
        except Exception as e:
            print(f"Error generating image: {e}")
            return None

def main():
    """Example usage of the simplified API"""
    
    # Initialize client
    client = GoesApiClient(BASE_URL)
    
    print("🛰️  data.pettusplots.com GOES API Client")
    print("=" * 50)
    
    # 1. Check server health
    print("\n1. Checking server health...")
    health = client.health_check()
    if health:
        print(f"✅ Server status: {health['status']}")
        print(f"📡 GOES available: {health['goes_available']}")
        print(f"📊 Total images: {health['total_images']}")
        if health['latest_image']:
            print(f"🕐 Latest image: {health['latest_image']}")
    
    # 2. List all available images
    print("\n2. Listing all GOES images...")
    images = client.list_all_images()
    if images and images['success']:
        print(f"📁 Found {images['count']} images on {images['domain']}")
        
        # Show the 5 most recent images
        for i, img in enumerate(images['images'][:5]):
            print(f"\n  {i+1}. {img['satellite']} - {img['sector']} - Band {img['band']}")
            print(f"     🔗 URL: {img['url']}")
            print(f"     🕐 Time: {img['timestamp']}")
            print(f"     📝 Text: {img['custom_text']}")
            print(f"     💾 Size: {img['size_kb']} KB")
        
        # Download the latest image
        if images['latest']:
            print(f"\n3. Downloading latest image...")
            latest_url = images['latest']['url']
            print(f"   URL: {latest_url}")
            
            downloaded = client.get_image_by_url(latest_url, "latest_goes.png")
            if downloaded:
                print(f"   ✅ Downloaded: {downloaded}")
    
    # 4. Generate a new image
    print(f"\n4. Generating new image...")
    custom_text = f"API Test {datetime.now().strftime('%H:%M')}"
    result = client.generate_new_image(custom_text)
    if result and result['success']:
        print(f"✅ Generated new image!")
        print(f"🔗 URL: {result['url']}")
        
        # Download the new image
        downloaded = client.get_image_by_url(result['url'], "new_goes.png")
        if downloaded:
            print(f"✅ Downloaded: {downloaded}")
    
    # 5. Download a specific image by descriptive path
    print(f"\n5. Testing direct URL access...")
    
    # Example descriptive URLs that might exist:
    test_urls = [
        "GOES19_FullDisk_Band13_CleanIR_Multichannel_20231201_1430Z",
        "GOES19_FullDisk_Band13_CleanIR_Multichannel_20231201_1445Z"
    ]
    
    for test_url in test_urls:
        print(f"   Trying: {test_url}")
        downloaded = client.get_image_by_url(test_url, f"test_{test_url}.png")
        if downloaded:
            print(f"   ✅ Success: {downloaded}")
            break
        else:
            print(f"   ❌ Not found")
    
    print("\n🎉 Demo completed!")

if __name__ == "__main__":
    main()

# Quick usage examples:

"""
# Basic usage:

from api_client import GoesApiClient

client = GoesApiClient("https://data.pettusplots.com")

# Check server status
health = client.health_check()
print(f"Status: {health['status']}")

# List all images
images = client.list_all_images()
print(f"Found {images['count']} images")

# Download latest image
if images['latest']:
    client.get_image_by_url(images['latest']['url'], "latest.png")

# Generate new image
result = client.generate_new_image("Hurricane Tracking")
if result['success']:
    client.get_image_by_url(result['url'], "hurricane.png")

# Download specific image by descriptive URL
client.get_image_by_url(
    "GOES19_FullDisk_Band13_CleanIR_Multichannel_20231201_1430Z", 
    "specific_image.png"
)

# Or use full URL
client.get_image_by_url(
    "https://data.pettusplots.com/goes/GOES19_FullDisk_Band13_CleanIR_Multichannel_20231201_1430Z",
    "full_url_image.png"
)
"""

# Example descriptive URLs you'll see:
"""
https://data.pettusplots.com/goes/GOES19_FullDisk_Band13_CleanIR_Multichannel_20231201_1430Z
https://data.pettusplots.com/goes/GOES18_FullDisk_Band13_CleanIR_Multichannel_20231201_1445Z
https://data.pettusplots.com/goes/GOES19_CONUS_Band13_CleanIR_Multichannel_20231201_1500Z

Format: /goes/{SATELLITE}_{SECTOR}_{BAND}_{PRODUCT}_{DATETIME}

Where:
- SATELLITE: GOES19, GOES18, GOES16
- SECTOR: FullDisk, CONUS, Mesoscale  
- BAND: Band13 (for your Clean IR script)
- PRODUCT: CleanIR_Multichannel, CleanIR_Radiance
- DATETIME: YYYYMMDD_HHMMZ (20231201_1430Z = Dec 1, 2023 14:30 UTC)
"""
