#!/usr/bin/env python3 

import pandas as pd 
import requests
import re 
import os 

def clean_text(text): 
    """
    Linguistic Normalization: Removes Wikipedia footnotes (wrapped in [] on HTML)  
    and cleans messy whitespace to ensure the LLM gets 'pure' strings
    """

    if not isinstance(text, str): 
        return str(text)
    
    #regex to find anything inside brackets and deleting it 
    text = re.sub(r'\[.*?\]', '', text)
    return text.strip()

def ingest_vocab_standards(): 
    print ("🧠 Initiating Wikipedia Scrape...")
    url = "https://es.wikipedia.org/wiki/Anexo:Diferencias_de_vocabulario_estándar_entre_países_hispanohablantes"

    #set a user agent 

    headers = { 
        'User-Agent' : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrom/114.0.0.0 Safari/537.36'
    }

    try: 
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status() 

        #using lxml bc it's highly optimized for M2 chip 

        all_tables = pd.read_html(response.text, flavor='lxml')
        print(f"🔎 Detected {len(all_tables)} total tables on page.")

        master_data = [] 

        for df in all_tables: 
            
            #clean headers, find 'País' 
            df.columns = [clean_text(str(col)) for col in df.columns]

            #find the 'País' column 
            pais_col = next((c for c in df.columns if 'País' in c or 'Pais' in c), None)

            if pais_col:
                df = df.rename(columns={pais_col: 'Country'})

                #filter out the Wikipedia Article rows that have no data 
                df = df[~df['Country'].str.contains('Artículo de Wikipedia', na=False)]

                #TRANSFORM from [Country | Bus | Plumber] into [Country | Concept | LocalTerm]
                melted = df.melt(id_vars=['Country'], var_name='Concept', value_name='LocalTerm')
                master_data.append(melted)

            if not master_data: 
                print("❌ FAILURE: Ingestion layer could not identify linguistic tables.")
                return
            
            #Concatenate and apply final filters 
            final_df = pd.concat(master_data, ignore_index=True).dropna()

            #clean every cell 

            for col in ['Country', 'Concept', 'LocalTerm']: 
                final_df[col] = final_df[col].apply(clean_text)
            
            #filter for the target markets 

            target_locales = ['México', 'Puerto Rico', 'Gautemala', 'España']
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