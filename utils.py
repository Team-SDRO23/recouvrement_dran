
import os, io,csv, re
from io import BytesIO,StringIO
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime
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
import re
import pandas as pd

def normalize_phone_ci(x):
    # Traite les manquants (None, np.nan, pd.NA, NaT, etc.)
    if pd.isna(x):
        return ''
    
    s = str(x).strip()

    if s == '' or s.lower() in {'nan', 'none', 'null', 'na', 'n/a', '#n/a'}:
        return ''

   
    s = re.sub(r'\+225', '', s, flags=re.IGNORECASE)

   
    s = re.sub(r'[\s\u00A0\.\-\(\)]', '', s)

    digits = re.sub(r'\D', '', s)

  
    if len(digits) < 8:
        return ''
    elif len(digits) == 8:
        return digits[-8:]
    elif len(digits) == 10:
        return digits[-10:]
    
    return ''


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
import pandas as pd



def _load_impayes(path: str) -> pd.DataFrame:
    imp = _read_any_file(path)
   
    if 'Ref contrat' in imp.columns and 'RefContrat' not in imp.columns:
        imp.rename(columns={'Ref contrat': 'RefContrat'}, inplace=True)
    if 'Téléphone privé' in imp.columns and 'Telephone_prive' not in imp.columns:
        imp.rename(columns={'Téléphone privé': 'Telephone_prive'}, inplace=True)
    if 'Téléphone professionnel' in imp.columns and 'Telephone_pro' not in imp.columns:
        imp.rename(columns={'Téléphone professionnel': 'Telephone_pro'}, inplace=True)
    if 'Numéro compteur' in imp.columns and 'Num_compteur' not in imp.columns:
        imp.rename(columns={'Numéro compteur': 'Num_compteur'}, inplace=True)
    if 'Total impayés échus en franc' not in imp.columns:
      
        for k in imp.columns:
            if 'impayés' in k and 'franc' in k:
                imp.rename(columns={k: 'Total impayés échus en franc'}, inplace=True)
                break
    return imp

def clean_num_compteur(df: pd.DataFrame) -> pd.DataFrame:

    df = df.copy()
    df['Num_compteur'] = df['Num_compteur'].astype(str).str.replace('nan', '')
    
    df['Num_compteur'] = df['Num_compteur'].apply(
        lambda x: x.split('_', 1)[-1] if '_' in x else x
    )
    return df


def _human_size(num_bytes):
    try:
        num_bytes = float(num_bytes)
    except Exception:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    while num_bytes >= 1024 and i < len(units) - 1:
        num_bytes /= 1024.0
        i += 1
    return f"{num_bytes:.1f} {units[i]}"


def _canon_key_str(x):
    s = re.sub(r"\D", "", str(x).strip())
    return pd.NA if s == "" else s



def _clean_telephone_col(df):
    # repère la colonne 'telephone' sans sensibilité à la casse
    cols = {c.lower(): c for c in df.columns}
    if 'telephone' in cols:
        col = cols['telephone']
        s = df[col]
        df[col] = (
            s.where(s.notna(), '')
             .astype(str)
             .replace(r'^\s*(nan|NaN|null|None)\s*$', '', regex=True)
             .str.replace(r'<br\s*/?>', ' ', regex=True)  # remplace <br> par espace
             .str.replace(r'\s+', ' ', regex=True)        # compact les espaces
             .str.strip()
        )
    return df

# def _dossier_sauvegarde() -> Path:
#     dossier = Path(app.config['SAVEPAYMENT_FOLDER']).resolve()
#     dossier.mkdir(parents=True, exist_ok=True)
#     return dossier


# def _verifier_chemin_secure(nom_fichier: str) -> Path:
#     """Vérifie et sécurise le chemin du fichier demandé."""
#     dossier = _dossier_sauvegarde()
#     nom_nettoye = secure_filename(nom_fichier)
#     if not nom_nettoye:
#         raise ValueError("Nom de fichier invalide.")
#     chemin = (dossier / nom_nettoye).resolve()
#     if dossier not in chemin.parents and chemin != dossier:
#         raise ValueError("Chemin de fichier non autorisé.")
#     return chemin


# @app.route('/sauvegardes', methods=['GET'])
# def lister_sauvegardes():
#     """Liste brute des fichiers dans la sauvegarde."""
#     try:
#         dossier = _dossier_sauvegarde()
#         fichiers = []
#         for f in sorted(dossier.iterdir()):
#             if f.is_file():
#                 stat = f.stat()
#                 fichiers.append({
#                     "nom": f.name,
#                     "taille": stat.st_size,  # en octets
#                     "modifie_le": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
#                 })

#         return render_template('sauvegardes.html', fichiers=fichiers)

#     except Exception as e:
#         flash(f"Erreur lors de la lecture des sauvegardes : {e}", category='danger')
#         return render_template('sauvegardes.html', fichiers=[])


# @app.route('/sauvegardes/telecharger/<path:nom_fichier>', methods=['GET'])
# def telecharger_sauvegarde(nom_fichier: str):
#     """Télécharge un fichier depuis la sauvegarde."""
#     try:
#         chemin = _verifier_chemin_secure(nom_fichier)
#         dossier = chemin.parent
#         return send_from_directory(
#             directory=str(dossier),
#             path=chemin.name,
#             as_attachment=True
#         )
#     except Exception as e:
#         flash(f"Téléchargement impossible : {e}", category='danger')
#         return redirect(url_for('lister_sauvegardes'))


# @app.route('/sauvegardes/supprimer', methods=['POST'])
# def supprimer_sauvegarde():
#     """Supprime un fichier sélectionné."""
#     nom_fichier = request.form.get('nom_fichier', '')
#     try:
#         chemin = _verifier_chemin_secure(nom_fichier)
#         if not chemin.exists() or not chemin.is_file():
#             flash("Fichier introuvable.", category='warning')
#             return redirect(url_for('lister_sauvegardes'))

#         os.remove(chemin)
#         flash(f"Fichier « {chemin.name} » supprimé avec succès.", category='success')
#     except Exception as e:
#         flash(f"Suppression impossible : {e}", category='danger')

#     return redirect(url_for('lister_sauvegardes'))
# {% extends "base.html" %}
# {% block content %}
# <div class="container py-3">
#   <h3>Fichiers de sauvegarde</h3>

#   {% with messages = get_flashed_messages(with_categories=true) %}
#     {% if messages %}
#       {% for category, message in messages %}
#         <div class="alert alert-{{ category }} my-2" role="alert">{{ message }}</div>
#       {% endfor %}
#     {% endif %}
#   {% endwith %}

#   {% if fichiers and fichiers|length > 0 %}
#     <table class="table table-sm table-striped align-middle">
#       <thead>
#         <tr>
#           <th>Nom</th>
#           <th>Taille (octets)</th>
#           <th>Modifié le</th>
#           <th class="text-end">Actions</th>
#         </tr>
#       </thead>
#       <tbody>
#         {% for f in fichiers %}
#           <tr>
#             <td>{{ f.nom }}</td>
#             <td>{{ f.taille }}</td>
#             <td>{{ f.modifie_le }}</td>
#             <td class="text-end">
#               <a class="btn btn-outline-primary btn-sm"
#                  href="{{ url_for('telecharger_sauvegarde', nom_fichier=f.nom) }}">
#                 Télécharger
#               </a>
#               <form method="post"
#                     action="{{ url_for('supprimer_sauvegarde') }}"
#                     style="display:inline-block"
#                     onsubmit="return confirm('Supprimer définitivement {{ f.nom }} ?');">
#                 <input type="hidden" name="nom_fichier" value="{{ f.nom }}">
#                 <button type="submit" class="btn btn-outline-danger btn-sm">Supprimer</button>
#               </form>
#             </td>
#           </tr>
#         {% endfor %}
#       </tbody>
#     </table>
#   {% else %}
#     <p>Aucun fichier dans la sauvegarde.</p>
#   {% endif %}
# </div>
# {% endblock %}
