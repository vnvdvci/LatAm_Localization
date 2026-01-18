#!/usr/bin/env python3 

#this script will be ingesting the Wikipedia database into a local file that the agent will consult
#avoids the need to browse the web every time 

import pandas as pd
import os 
import requests
from bs4 import BeautifulSoup
import re 

#TASK: take the Wikipedia page that houses all vocabulary differences in Spanish and save it as a CSV 

def clean_text(text):
    """ Removes all text enclosed in brackets ie links [29] [3]"""
    if not isinstance(text, str):
        return str(text)
    text =  re.sub(r'\[.*?\]','',text)

    return text.strip() #removes all text enclosed in square brackets 

def ingest_vocab_standards():
    print("🚀 🚀  Ingesting Wikipedia Linguistic Standards ... ")
    url = "https://es.wikipedia.org/wiki/Anexo:Diferencias_de_vocabulario_estándar_entre_países_hispanohablantes"

    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')

        #find all the tables in the website 

        tables = soup.find_all('table', {'class' : 'wikitable'})
        print(f"aha!🔎 Found {len(tables)} linguistic tables on this page")

        master_data = [] 

        for i, table in enumerate(tables): 
            #convert the html table to dataframe 
            df = pd.read_html(str(table))[0]

            df.columns = [clean_text(str(col)) for col in df.columns]

            #ID País colum 

            pais_col = next((c for c in df.columns if 'País' in c or 'Artículo' in c), None)

            if not pais_col: 
                continue 

            #rename País column to 'Country' 
            df = df.rename(columns={pais_col: 'Country'})
            
            #remove rows that are 'Wikipedia Article' links 
            df = df[~df['Country'].str.contains('Artículo de Wikipedia', na=False)]

            #[Country | Plumber | Bus] --> [Country | Concept | LocalTerm]
            #Task: use melting to turn columns into rows 

            melted = df.melt(id_vars=['Country'], var_name='Concept', value_name='LocalTerm')
            master_data.append(melted)

        #combine all the tables into one master dataset 

        final_df = pd.concat(master_data, ignore_index=True)

        #remove empty rows and apply string cleaning to all cells 

        final_df = final_df.dropna() 
        final_df['Country'] = final_df['Country'].apply(clean_text)
        final_df['Concept'] = final_df['Concept'].apply(clean_text)
        final_df['LocalTerm'] = final_df['LocalTerm'].apply(clean_text)

        #filter: MX, PR, GT, ES

        target_locales = ['México', 'Puerto Rico', 'Guatemala', 'España']
        final_df = final_df[final_df['Country'].isin(target_locales)]

        #save all of this into folder 

        os.makedirs('data/processed', exist_ok=True)
        final_df.to_csv("data/processed/regional_vocab.csv", index=False)

        print(f"✅ SUCCESS: {len(final_df)} precision mapping rules saved to data/processed/regionla_vocab.csv")
        print("\n --- PREVIEW OF YOUR KNOWLEDGE ORACLE ---")
        print(final_df.sample(5).to_string(index=False))

    except Exception as e: 
        print(f"❌ CRITICAL ERROR during ingestion: {e}")
        
if __name__ == "__main__":
    ingest_vocab_standards()







