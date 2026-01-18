#!/usr/bin/env python3 

import pandas as pd 
import requests
import re 
import os 
import io 

def clean_text(text): 
    """
    Linguistic Normalization: Removes Wikipedia footnotes (wrapped in [] on HTML)  
    and cleans messy whitespace to ensure the LLM gets 'pure' strings
    """

    if not isinstance(text, str): 
        return str(text)
    
    #regex to find anything inside brackets and deleting it 
    text = re.sub(r'\[.*?\]', '', text)
    #remove non-breaking spaces and weird whitespace 
    text = text.replace('\xa0', ' ') 
    return text.strip()

def ingest_vocab_standards(): 
    print ("🧠 Initiating Wikipedia Scrape...")
    url = "https://es.wikipedia.org/wiki/Anexo:Diferencias_de_vocabulario_estándar_entre_países_hispanohablantes"

    #set a user agent 

    headers = { 
        'User-Agent' : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
    }

    try: 
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status() 

        #wrap in StringIO 
        html_data = io.StringIO(response.text)
    
        #using lxml bc it's highly optimized for M2 chip 

        all_tables = pd.read_html(response.text, flavor='lxml')
        print(f"🔎 Detected {len(all_tables)} total tables on page.")

        master_data = [] 

        for i, df in enumerate(all_tables): 
            # converting MultiIndex columns to flat strings (if they exist)

            if isinstance(df.columns, pd.MultiIndex): 
                df.columns = [' '.join(col).strip() for col in df.columns.values]

            #clean all column names 
            df.columns = [clean_text(str(col)) for col in df.columns]

            cols_str = " ".join(df.columns).lower() 

            if 'país' in cols_str or 'pais' in cols_str: 
                #find the exact name of that column 
                actual_col = [c for c in df.columns if 'pais' in c.lower()][0]

                df = df.rename(columns={actual_col: 'Country'})

                #filter out 'Wiki Articles' rows 
                df = df[~df['Country'].str.contains('Artículo de Wikipedia', na=False,case=False)]

                #melt table 
                melted = df.melt(id_vars=['Country'], var_name='Concept', value_name='LocalTerm')
                master_data.append(melted)

            

        if not master_data: 
            print("❌ FAILURE: still no linguistic tables identified")
            #Debug: show columns of the first 5 tables to see what we're missing 

            print("Debug - Columns of table 0:", all_tables[0].columns.tolist())
            return
            
            final_df = pd.concat(master_data, ignore_index=True).dropna()

            #clean every cell 

            for col in ['Country', 'Concept', 'LocalTerm']: 
                final_df[col] = final_df[col].apply(clean_text)
            
            #filter for the target markets 

            target_locales = ['México', 'Puerto Rico', 'Guatemala', 'España']
            final_df = final_df[final_df['Country'].isin(target_locales)]

            #save to centralized data directory 
            output_dir = 'data/processed'
            os.makedirs(output_dir, exist_ok=True)
            output_path = os.path.join(output_dir, 'regional_vocab.csv')
            
            final_df.to_csv(output_path, index=False)

            print(f"✅ SUCCESS: {len(final_df)} precision mapping rules saved to {output_path}")
            print("\n--- SAMPLE OF AGENT-READY KNOWLEDGE---")
            print(final_df.sample(min(5, len(final_df))).to_string(index=False))

   
    except Exception as e: 
      print(f"❌ CRITICAL ERROR during V2 Ingestion: {e}")

if __name__ == "__main__": 
    ingest_vocab_standards() 