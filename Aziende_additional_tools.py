import pandas as pd
import numpy as np
import os 
from tqdm import tqdm
from fuzzywuzzy import fuzz, process


piani_aziendali = pd.read_parquet('data/piani_aziendali.gzip').dropna(subset='Label 0').drop(columns='ID_tassonomia')  
piani_aziendali['Label 0'] = piani_aziendali['Label 0'].astype(int)

tassonomia = piani_aziendali[['Label 0','codice_macro', 
                 'descrizione_codice_macro', 'numero_codice_campo',
                'descrizione_codice_campo']].value_counts().reset_index().rename(columns={'Label 0':'ID_tassonomia'}
                                                                                ).sort_values(by='ID_tassonomia'
                                                                                             ).rename(columns={'count':'Frequenza della combinazione nell\'intero dataset'})

c = []
for i in tassonomia[['ID_tassonomia', 'codice_macro', 'numero_codice_campo']].values:    
    view = piani_aziendali[(piani_aziendali['Label 0'] == i[0]) & (piani_aziendali['codice_macro'] == i[1]) & (piani_aziendali['numero_codice_campo'] == i[2])]
    c.append(view.ID_organizzazione.unique().shape[0])
tassonomia['Numero di organizzazioni che ne hanno fatto uso nel dataset'] = c
#######################################
# Azioni ordinate per utilizzo di UNA organizzazione (57ms)

# e.g. '12345055' o '5055'
print('L\'ID dell\'organizzazione si trova più facilmente se si specificano le ultime 4 cifre dell\'identificativo.')
user_input = input("ID dell'organizzazione da analizzare (inserire l\'ID specifico di una organizzazione coinvolta):") 
choices = piani_aziendali.ID_organizzazione.astype(str).unique()  

azienda = process.extractOne(str(user_input.strip()), choices, scorer=fuzz.ratio)[0]
focus = piani_aziendali[piani_aziendali.ID_organizzazione==azienda]

rank = focus.value_counts(subset=['Label 0', 'codice_macro', 'descrizione_codice_macro', 'numero_codice_campo', 'descrizione_codice_campo']).reset_index().rename(columns=
                                                                            {'Label 0':'ID_tassonomia', 
                                                                             'count':'Frequenza nell\'organizzazione'})

rank = pd.merge(rank, tassonomia[['ID_tassonomia', 
                                  'Frequenza della combinazione nell\'intero dataset', 
                                  'Numero di organizzazioni che ne hanno fatto uso nel dataset']], how='left')
rank.to_csv(f'{fp}{azienda}.csv')
print(f'Le frequenze di azioni {fp}{azienda}.csv')
print(rank.head())

#######################################
# Serie storica per una lista di voci della tassonomia selezionate dall'utente (125ms)
    # e.g. di input precompilato:
# user_inputs = [11630769,
#                11630764,
#                11630751,
#                11630770]               

user_inputs = []

while True:
    entry = input("Azione da visualizzare (scrivi 'END' per terminare la lista): ")
    if entry.strip() == "END":
        break
    user_inputs.append(entry.strip())

print("Voci inserite:", user_inputs)
    

choices = tassonomia.ID_tassonomia

    # identificazione degli input tra le scelte possibili
if len(user_inputs) > 1:
    azione = [process.extract(str(a), choices)[0][0] for a in user_inputs ]
elif len(user_inputs): 
    azione = [process.extract(str(user_inputs[0]), choices)[0][0]]

    # preparazione dati per la visualizzazione
def wrapper(text, words_per_line=3):    
    words = text.replace('/',' - ').split()
    lines_per_chunk = words_per_line%len(words) + 1  
    lines = [' '.join(words[i:i+words_per_line]) for i in range(0, len(words), words_per_line)]    
    chunks = ['<br>'.join(lines[i:i+lines_per_chunk]) for i in range(0, len(lines), lines_per_chunk)]    
    return chunks[0]
    
storia = pd.DataFrame(columns=['anno_compilazione'])
for a in azione:
    t = piani_aziendali[piani_aziendali['Label 0'] == a]
    #si deve "rompere" a causa della frequente lunghezza eccessiva
    try:
        a = wrapper(a)
    except:
        a
    t = t.anno_compilazione.value_counts().reset_index().rename(columns={'count':a})
    
    if storia.shape[0]==0:
        storia = t
    else:
        storia = pd.merge(storia, t, on='anno_compilazione', how='left')    
storia = storia.fillna(0).sort_values(by='anno_compilazione')
storia.to_csv(f'{fp}serie_storica.csv')
print(f'Serie storica per le azioni selezionate salvata in {fp}serie_storica.csv')
print(storia.head())
print()
    # visualizzazione 
import plotly.express as px

df_long = storia.melt(id_vars='anno_compilazione', value_vars=storia,
                 var_name='Voce della tassonomia', value_name='Value')
fig = px.line(
    df_long,
    x='anno_compilazione',
    y='Value',
    color='Voce della tassonomia',
    markers=True,
)
    
fig.update_layout(
    xaxis_title="Annualità",
    yaxis_title="Frequenza di utilizzo"
)
fig.write_html(f"{fp}serie_storica_azione.html")
print(f'Visualizzazione interattiva salvata in {fp}serie_storica_azione.html')
