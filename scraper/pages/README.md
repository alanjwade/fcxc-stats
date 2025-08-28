# Local HTML File Processing

This directory (`scraper/pages/`) is used to store downloaded HTML files from MileSplit or other race results websites.

## How to Use

1. **Download Race Results Page**:
   - Go to the MileSplit results page in your browser
   - Right-click and select "Save page as..." or use Ctrl+S
   - Save the HTML file to this directory (e.g., `meet_name_2024.html`)

2. **Update Configuration**:
   - Edit `config/races.yaml`
   - Add a race entry with the `file` field pointing to your saved HTML file
   - Example:
     ```yaml
     - name: "State Championship 2024"
       file: "pages/state_championship_2024.html"
       distance: "5K"
       race_class: "varsity"
       gender: "boys"
       venue: "State Park"
       date: "2024-11-15"
       season: "2024"
     ```

3. **Run the Scraper**:
   - The scraper will automatically detect local files and parse them
   - No internet connection needed for local file processing

## Tips

- Use descriptive filenames that match your race names
- Keep the original HTML structure intact for better parsing
- If you have both `url` and `file` specified, `file` takes precedence
- Test with the sample file first: `pages/sample_results.html`
- **Note**: HTML files in this directory are excluded from git tracking to avoid committing large source files

## File Structure

```
scraper/pages/
├── README.md                    # This file
├── sample_results.html          # Sample file for testing
└── your_race_results.html       # Your downloaded race results
```
