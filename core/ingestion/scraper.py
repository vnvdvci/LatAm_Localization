import trafilatura

def get_url_content(url): 
    """
    Extracts only the 'meat' of the page, ignores navbars, ads, and footers
    to save tokens when we pass this data to llama 3
    """

    print(f"🌐 Scrapping: {url}...")

    try: 
        downloaded = trafilatura.fetch_url(url)
        if not downloaded: 
            return "Error: Could not access the URL."
        
        #extract main content while preserving tables 
        content = trafilatura.extract(downloaded, include_comments=False, include_tables=True)

        if content: 
            print(f"✅ Success: Extracted {len(content)} characters.")
            return content 
        else: 
            return "Error: No readable text found"
    except Exception as e: 
        return f"CRITICAL SCRAPE ERROR: {e}"
    
if __name__ == "__main__": 
    test_url = "https://decagon.ai/resources/10-principles-of-a-production-grade-voice-ai-agent"
    text = get_url_content(test_url)
    print("\n--- SCRAPE PREVIEW ---")
    print(text[:500]+ "...")