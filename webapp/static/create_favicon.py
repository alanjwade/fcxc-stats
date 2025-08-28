#!/usr/bin/env python3
"""Generate a favicon for Fort Collins Cross Country Stats"""

from PIL import Image, ImageDraw

def create_favicon():
    # Create a 32x32 image with transparent background
    size = 32
    img = Image.new('RGBA', (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    
    # Fort Collins colors
    purple = (102, 51, 153)  # #663399
    gold = (255, 215, 0)     # #FFD700
    
    # Draw background circle
    draw.ellipse([2, 2, size-2, size-2], fill=purple, outline=gold, width=2)
    
    # Draw a simple running figure
    # Head
    draw.ellipse([11, 6, 17, 12], fill=gold)
    
    # Body
    draw.rectangle([13, 12, 15, 20], fill=gold)
    
    # Arms (simple lines)
    draw.line([8, 14, 13, 15], fill=gold, width=2)
    draw.line([15, 15, 20, 13], fill=gold, width=2)
    
    # Legs
    draw.line([13, 20, 10, 26], fill=gold, width=2)
    draw.line([15, 20, 18, 26], fill=gold, width=2)
    
    # Track lines at bottom
    draw.arc([4, 24, 28, 30], 0, 180, fill=gold, width=1)
    
    return img

if __name__ == "__main__":
    # Create the favicon
    favicon = create_favicon()
    
    # Save as PNG
    favicon.save('/home/alan/Documents/code/fcxc_stats/webapp/static/favicon-32x32.png', 'PNG')
    
    # Create 16x16 version
    favicon_16 = favicon.resize((16, 16), Image.Resampling.LANCZOS)
    favicon_16.save('/home/alan/Documents/code/fcxc_stats/webapp/static/favicon-16x16.png', 'PNG')
    
    # Create ICO file with multiple sizes
    favicon.save('/home/alan/Documents/code/fcxc_stats/webapp/static/favicon.ico', 
                format='ICO', sizes=[(16, 16), (32, 32)])
    
    print("Favicon files created successfully!")
