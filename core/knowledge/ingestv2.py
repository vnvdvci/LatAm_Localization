import pandas as pd
import requests
import re
import os
import io
import unicodedata

def normalize_caseless(text):
    """
    The 'Senior Lead' Secret: This function strips accents and 
    special characters, turning 'País' into 'pais' for foolproof searching.
    """
    if not isinstance(text, str): return str(text)
    # Remove accents/diacritics
    text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8')
    return text.lower().strip()

def clean_text(text):
    """Standard Wikipedia cleanup."""
    if not isinstance(text, str): return str(text)
    text = re.sub(r'\[.*?\]', '', text)
    text = text.replace('\xa0', ' ')
    return text.strip()

def ingest_vocab_standards():
    print("🧠 Senior Lead Logic: Initiating V2.3 (Unicode-Safe Ingestion)...")
    url = "https://es.wikipedia.org/wiki/Anexo:Diferencias_de_vocabulario_estándar_entre_países_hispanohablantes"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
    }

    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()

        # Fix FutureWarning: Wrap in StringIO
        html_data = io.StringIO(response.text)
        all_tables = pd.read_html(html_data, flavor='lxml')
        print(f"🔎 Detected {len(all_tables)} total tables on page.")

        master_data = []

        for df in all_tables:
            # Flatten MultiIndex if present
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [' '.join(col).strip() for col in df.columns.values]
            
            # Clean original column names for the final CSV
            df.columns = [clean_text(str(col)) for col in df.columns]
            
            # Create a list of normalized column names just for the search
            norm_cols = [normalize_caseless(c) for c in df.columns]
            
            # Check if 'pais' exists in normalized columns
            if 'pais' in norm_cols:
                # Get the index of the 'pais' column
                target_idx = norm_cols.index('pais')
                # Get the actual name used in the dataframe
                actual_col_name = df.columns[target_idx]
                
                # Standardize to 'Country'
                df = df.rename(columns={actual_col_name: 'Country'})
                
                # Filter out the meta-header rows
                df = df[~df['Country'].str.contains('Artículo de Wikipedia', na=False, case=False)]
                
                # MELT: Transform matrix to relational list
                melted = df.melt(id_vars=['Country'], var_name='Concept', value_name='LocalTerm')
                master_data.append(melted)

        if not master_data:
            print("❌ FAILURE: No linguistic tables could be normalized.")
            return

        # Combine, clean, and filter
        final_df = pd.concat(master_data, ignore_index=True).dropna()
        
        for col in ['Country', 'Concept', 'LocalTerm']:
            final_df[col] = final_df[col].apply(clean_text)

        # Ensure Guatemala is spelled correctly here!
        target_locales = ['México', 'Puerto Rico', 'Guatemala', 'España']
        final_df = final_df[final_df['Country'].isin(target_locales)]

        # Persistence
        os.makedirs('data/processed', exist_ok=True)
        output_path = 'data/processed/regional_vocab.csv'
        final_df.to_csv(output_path, index=False)
        
        print(f"✅ SUCCESS: {len(final_df)} rules saved to {output_path}")
        print("\n--- SAMPLE OF YOUR KNOWLEDGE ORACLE ---")
        print(final_df.sample(min(10, len(final_df))).to_string(index=False))

    except Exception as e:
        print(f"❌ CRITICAL ERROR: {e}")

if __name__ == "__main__":
    ingest_vocab_standards()