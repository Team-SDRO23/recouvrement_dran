
import os, io,csv, re
from io import BytesIO,StringIO
import numpy as np
import pandas as pd
from pathlib import Path

def _read_any_file(path: str) -> pd.DataFrame:

    ext = Path(path).suffix.lower()
    if ext in ('.xlsx', '.xls'):
        df = pd.read_excel(path, sheet_name=0, dtype=str, keep_default_na=False)
    elif ext == '.csv':
        # lecture robuste CSV: encodage + séparateur
        with open(path, 'rb') as f:
            raw = f.read()
        # encodage
        for enc in ('utf-8-sig', 'utf-8', 'cp1252', 'latin-1'):
            try:
                text = raw.decode(enc)
                encoding_used = enc
                break
            except UnicodeDecodeError:
                continue
        else:
            text = raw.decode('utf-8', errors='replace')
            encoding_used = 'utf-8'
        # séparateur
        sample = text[:10000]
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
            sep = dialect.delimiter
        except Exception:
            sep = ','
        df = pd.read_csv(StringIO(text), dtype=str, keep_default_na=False,
                         sep=sep, engine='python')
    else:
        raise ValueError(f"Format non supporté: {ext}")
    df.rename(columns=lambda c: str(c).strip(), inplace=True)
    return df

def _to_number_cfa(x: str):
   
    if x is None:
        return pd.NA
    s = str(x)

    s = re.sub(r'\s|\u00A0', '', s)
    s = s.replace('CFA', '')
    s = s.replace('.', '').replace(',', '')
    return pd.to_numeric(s, errors='coerce')
import re
import pandas as pd

def normalize_phone_ci(x):

    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ''
    
    s = str(x)

    # 1) supprimer +225
    s = re.sub(r'\+225', '', s, flags=re.IGNORECASE)

    s = re.sub(r'[\s\u00A0\.\-\(\)]', '', s)

    # 3) garder uniquement les chiffres
    digits = re.sub(r'\D', '', s)

    # 4) règles de validation
    if len(digits) < 8:       
        return ''
    elif len(digits) == 8:
        return digits[-8:]
    elif len(digits) == 10:   
        return digits[-10:]


def _load_all_payments(folder: str) -> pd.DataFrame:
    frames = []
    for name in os.listdir(folder):
        if not name.lower().endswith(('.xlsx', '.xls', '.csv')):
            continue
        path = os.path.join(folder, name)
        try:
            df = _read_any_file(path)
           
            if 'Ref contrat' in df.columns and 'RefContrat' not in df.columns:
                df.rename(columns={'Ref contrat': 'RefContrat'}, inplace=True)
          
            if 'RefContrat.1' in df.columns:
                if 'RefContrat' not in df.columns:
                    df.rename(columns={'RefContrat.1': 'RefContrat'}, inplace=True)
                else:
                    same = (df['RefContrat'].fillna('') == df['RefContrat.1'].fillna('')).all()
                    if same:
                        df.drop(columns=['RefContrat.1'], inplace=True)
            frames.append(df)
        except Exception as e:
          
            print(f"[WARN] Lecture paiement échouée pour {name}: {e}")
    if not frames:
        return pd.DataFrame()
    pay = pd.concat(frames, ignore_index=True, sort=False)
   
    for c in ['RefContrat', 'RefFacture', 'DateCreation', 'MontantReglement']:
        if c in pay.columns:
            pay[c] = pay[c].astype(str)
    return pay

def _load_impayes(path: str) -> pd.DataFrame:
    imp = _read_any_file(path)
   
    if 'Ref contrat' in imp.columns and 'RefContrat' not in imp.columns:
        imp.rename(columns={'Ref contrat': 'RefContrat'}, inplace=True)
    if 'Téléphone privé' in imp.columns and 'Telephone_prive' not in imp.columns:
        imp.rename(columns={'Téléphone privé': 'Telephone_prive'}, inplace=True)
    if 'Téléphone professionnel' in imp.columns and 'Téléphone professionnel' not in imp.columns:
        imp.rename(columns={'Téléphone professionnel': 'Telephone_pro'}, inplace=True)
    if 'Solde Total factures échues' not in imp.columns:
      
        for k in imp.columns:
            if 'Solde' in k and 'échue' in k:
                imp.rename(columns={k: 'Solde Total factures échues'}, inplace=True)
                break
    return imp

