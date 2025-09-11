from flask import (
    Flask, render_template, request, redirect, url_for, session, flash, make_response,
    send_file, send_from_directory
)
from werkzeug.utils import secure_filename
from werkzeug.middleware.proxy_fix import ProxyFix
import os, io, csv, re
from io import BytesIO, StringIO
import numpy as np
import pandas as pd
from flask_sqlalchemy import SQLAlchemy
from pathlib import Path
from datetime import datetime
from functools import wraps
from utils import (
    _read_any_file, _to_number_cfa, _load_all_payments, _load_impayes,
    normalize_phone_ci, clean_num_compteur, _human_size, _canon_key_str,
    _clean_telephone_col
)
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import getSampleStyleSheet
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from zoneinfo import ZoneInfo
from time import time
from werkzeug.exceptions import RequestEntityTooLarge
from tempfile import NamedTemporaryFile
import shutil

# -------------------------------------------------------
# Config
# -------------------------------------------------------
ALLOWED_PAYMENT_EXTS = {'.xlsx', '.xls', '.csv'}

app = Flask(__name__)
app.secret_key = 'secret_key'



BASE_DIR = Path(__file__).resolve().parent

app.config['UPLOAD_FOLDER']      = str(BASE_DIR / 'impayefacture')
app.config['PAYMENT_FOLDER']     = str(BASE_DIR / 'payementfacture')
app.config['SAVEPAYMENT_FOLDER'] = str(BASE_DIR / 'sauvegardepayement')

Path(app.config['UPLOAD_FOLDER']).mkdir(parents=True, exist_ok=True)
Path(app.config['PAYMENT_FOLDER']).mkdir(parents=True, exist_ok=True)
Path(app.config['SAVEPAYMENT_FOLDER']).mkdir(parents=True, exist_ok=True)


app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///users.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1)
app.config.update(
    PREFERRED_URL_SCHEME='https',      # tu sers en HTTPS côté front
    SESSION_COOKIE_SECURE=True,        # cookie secure (reco en prod)
    SESSION_COOKIE_SAMESITE='Lax',     # défaut sûr pour navigation
    # SESSION_COOKIE_DOMAIN='dran.dxteriz.com',  # pas nécessaire sauf sous-domaines
)
if app.debug or os.environ.get("FLASK_ENV") == "development":
    app.config['SESSION_COOKIE_SECURE'] = False

app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024
db = SQLAlchemy(app)


# -------------------------------------------------------
# Outils Débogage
# -------------------------------------------------------
@app.before_request
def _dbg_session():
    try:
        print("Cookie header:", request.headers.get('Cookie'))
        print("Flask session keys:", list(session.keys()))
        print("secteur in session? ", 'secteur' in session)
    except Exception as e:
        print("DBG error:", e)

@app.after_request
def _log_redirects(resp):
    try:
        if 300 <= resp.status_code < 400:
            print(f">> AFTER_REQUEST redirect {request.method} {request.path} "
                  f"→ {resp.status_code} Location={resp.headers.get('Location')}")
    finally:
        return resp

@app.before_request
def _dbg_route():
    print(f">> BEFORE_REQUEST {request.method} {request.path} Cookie?={'session' in (request.headers.get('Cookie') or '')}")

def _no_store(resp):
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    resp.headers['Pragma'] = 'no-cache'
    resp.headers['Vary'] = 'Cookie'
    return resp

def _see_other(endpoint, **values):
    # 303 See Other pour un PRG propre
    return redirect(url_for(endpoint, **values), code=303)

# -------------------------------------------------------
# Outils Secteur
# -------------------------------------------------------
def _tz_now():
    return datetime.now(ZoneInfo('Africa/Abidjan')) if ZoneInfo else datetime.now()

def _active_secteur() -> str:
    return (session.get('secteur') or '').strip()

def _require_secteur(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not _active_secteur():
            flash("Choisissez d’abord un secteur.", "warning")
            return redirect(url_for('choisir_secteur'))
        return fn(*args, **kwargs)
    return wrapper

def _safe_secteur_dir(name: str) -> Path:
    """Dossier sûr pour un secteur (utilisé pour impayés et sauvegardes)."""
    safe = secure_filename(name or 'inconnu') or 'inconnu'
    return Path(safe)

def _impaye_path_for_sector(secteur: str) -> Path:
    root = Path(app.config['UPLOAD_FOLDER']).resolve()
    folder = (root / _safe_secteur_dir(secteur)).resolve()
    folder.mkdir(parents=True, exist_ok=True)
    return folder / 'factureimpaye.xlsx'

def _savebook_path_for_sector(secteur: str) -> Path:
    root = Path(app.config['SAVEPAYMENT_FOLDER']).resolve()
    folder = (root / _safe_secteur_dir(secteur)).resolve()
    folder.mkdir(parents=True, exist_ok=True)
    return folder / 'clients_eligibles.xlsx'

def _list_secteurs() -> list:
    """Liste des secteurs distincts trouvés dans les fichiers de paiements."""
    pf = app.config['PAYMENT_FOLDER']
    if not os.path.isdir(pf):
        return []
    try:
        pay = _load_all_payments(pf)
    except Exception:
        return []
    if pay.empty or 'Secteurs' not in pay.columns:
        return []
    s = (pay['Secteurs'].astype(str).str.strip()
         .replace({'nan': '', 'NaN': '', 'None': '', 'NULL': ''}))
    s = s[s != '']
    return sorted(s.dropna().unique().tolist())


SECTEURS_FIXES = [
    "Adjamé -Nord", "Adjamé - Sud", "Bingerville",
    "2 Plateaux", "Cocody", "Djibi"
]
SECT_ABBR = {
    'Adjamé -Nord': 'Adj-N',
    'Adjamé - Sud':   'Adj-S',
    'Bingerville':    'Bing.',
    '2 Plateaux':     '2P',
    'Cocody':         'Coc.',
    'Djibi':          'Djibi',
}
@app.template_filter('sect_abbr')
def sect_abbr(name):
    key = (name or '').strip()
    return SECT_ABBR.get(key, key or 'Inconnu')

# -------------------------------------------------------
# Accueil & choix secteur
# -------------------------------------------------------
@app.route('/')
def home():
   
    if not _active_secteur():
        return redirect(url_for('choisir_secteur'))
  
    return redirect(url_for('upload_liste_impaye'))

@app.route('/secteurs', methods=['GET'])
def choisir_secteur():
    welcome = "Bienvenue sur l’appli de rétablissement des clients de la DRAN"
    return render_template('secteurs.html', secteurs=SECTEURS_FIXES, welcome_msg=welcome)

# --- Définir le secteur choisi ---
@app.route('/set_secteur', methods=['GET'])
def set_secteur():
    s = request.args.get('secteur', '').strip()
    if s not in SECTEURS_FIXES:
        flash("Secteur invalide.", "warning")
        return redirect(url_for('choisir_secteur'))
    session['secteur'] = s
    flash(f"Secteur sélectionné : {s}", "success")
    return redirect(url_for('upload_liste_impaye'))


def find_header_row(df0, required_labels=None, min_match=3):
    if required_labels:
        req = {s.lower().strip() for s in required_labels}
        for i in range(len(df0)):
            vals = {
                str(x).lower().strip()
                for x in df0.iloc[i].tolist()
                if pd.notna(x) and str(x).strip() != ""
            }
            if len(req.intersection(vals)) >= min_match:
                return i
    non_na_counts = df0.notna().sum(axis=1)
    return int(non_na_counts.idxmax())


@app.errorhandler(RequestEntityTooLarge)
def file_too_large(e):
    flash("Fichier trop volumineux (au-delà de la limite configurée).", "danger")
    return redirect(url_for('upload_liste_impaye'))

@app.route('/upload_liste_impaye', methods=['GET', 'POST'])
@_require_secteur
def upload_liste_impaye():
    secteur = _active_secteur()
    preview_html = None
    orig_filename = request.args.get('orig')

    if request.method == 'POST':
        app.logger.info("=== UPLOAD START ===")
        app.logger.info("Headers Content-Type: %s", request.headers.get('Content-Type'))
        app.logger.info("FILES keys: %s", list(request.files.keys()))
        app.logger.info("FORM keys: %s", list(request.form.keys()))

        f = request.files.get('file')
        if not f:
            lst = request.files.getlist('files')
            f = lst[0] if lst else None

        if not f or not getattr(f, 'filename', ''):
            flash("Veuillez choisir un fichier (.xlsx, .xls ou .csv).", "warning")
            return _no_store(_see_other('upload_liste_impaye', t=int(time.time())))

        orig_filename = secure_filename(f.filename)
        ext = ('.' + orig_filename.rsplit('.', 1)[-1]).lower() if '.' in orig_filename else ''
        if ext not in ('.xlsx', '.xls', '.csv'):
            flash("Format non supporté.", "warning")
            return _no_store(_see_other('upload_liste_impaye', t=int(time.time())))

        try:
            # Sauvegarde d’abord dans un fichier temporaire
            with NamedTemporaryFile(delete=False, suffix=ext) as tmp:
                tmp_name = tmp.name
                f.stream.seek(0)
                shutil.copyfileobj(f.stream, tmp)
            app.logger.info("Upload sauvegardé dans le tmp: %s", tmp_name)

            # Lecture du fichier via pandas
            if ext == '.csv':
                df0 = pd.read_csv(tmp_name, header=None, dtype=object)
            elif ext in ('.xlsx', '.xls'):
                try:
                    df0 = pd.read_excel(tmp_name, sheet_name=0, header=None, dtype=object, engine='openpyxl')
                except Exception:
                    df0 = pd.read_excel(tmp_name, sheet_name=0, header=None, dtype=object)

            # Trouver ligne d’en-tête
            required = ['Matricule AZ', 'Nom AZ', 'Tournee', 'Genre client']
            header_row = find_header_row(df0, required_labels=required, min_match=3)

            header = [str(x).strip() if pd.notna(x) else "" for x in df0.iloc[header_row].tolist()]
            df = df0.iloc[header_row + 1:].copy()
            df.columns = header

            # Nettoyage colonnes
            df = df.replace(r'^\s*$', pd.NA, regex=True)
            colnames = list(df.columns)
            mask_bad = np.array([
                (c is None) or (str(c).strip() == "") or str(c).lower().startswith("unnamed")
                for c in colnames
            ], dtype=bool)
            if mask_bad.any():
                df = df.iloc[:, ~mask_bad]

            df = df.dropna(axis=1, how='all').dropna(axis=0, how='all')
            df.columns = [str(c).strip() for c in df.columns]
            df = df.loc[:, ~pd.Index(df.columns).duplicated()]

            # Sauvegarde finale normalisée
            save_path: Path = _impaye_path_for_sector(secteur)
            if save_path.suffix.lower() not in ('.xlsx', '.xls'):
                save_path = save_path.with_suffix('.xlsx')
            save_path.parent.mkdir(parents=True, exist_ok=True)

            try:
                df.to_excel(save_path, index=False, engine='openpyxl')
            except TypeError:
                df.to_excel(save_path, index=False)

            flash(f"Fichier d’impayés « {orig_filename} » enregistré pour le secteur « {secteur} ».", "info")
            return _no_store(_see_other('upload_liste_impaye', t=int(time.time()), orig=orig_filename))

        except Exception as e:
            app.logger.exception("Erreur durant upload/processing")
            flash(f"Erreur lors du traitement du fichier : {e}", "danger")
            return _no_store(_see_other('upload_liste_impaye', t=int(time()), orig=orig_filename))
        finally:
            try:
                if tmp_name and os.path.exists(tmp_name):
                    os.remove(tmp_name)  
            except Exception:
                pass

    try:
        save_path: Path = _impaye_path_for_sector(secteur)
        if save_path.exists():
            if save_path.suffix.lower() == '.csv':
                df = pd.read_csv(save_path, dtype=object)
            else:
                try:
                    xls = pd.ExcelFile(save_path)
                    df = xls.parse(xls.sheet_names[0], dtype=object) if xls.sheet_names else pd.DataFrame()
                except Exception:
                    df = pd.read_excel(save_path, dtype=object)

            if not df.empty:
                df_preview = df.iloc[:10, :5].fillna("")
                preview_html = df_preview.to_html(
                    classes="table table-sm table-striped table-bordered align-middle",
                    index=False, border=0, escape=False
                )
                if not orig_filename:
                    orig_filename = save_path.name
    except Exception as e:
        app.logger.exception("Erreur lors de la lecture du fichier impayés existant")
        preview_html = None

    resp = make_response(render_template(
        'upload_liste_impaye.html',
        preview_html=preview_html,
        orig_filename=orig_filename
    ), 200)
    return _no_store(resp)




@app.route('/upload_liste_payement', methods=['GET','POST'])
def upload_liste_payement():

    data_preview_html, uploaded_names = None, []
    added_rows, duplicates_removed = 0, 0

    if request.method == 'POST':
        file = request.files.get('file') or (request.files.getlist('files') or [None])[0]
        if not file:
            flash("Veuillez choisir un fichier (.xlsx, .xls ou .csv).", "info")
            return render_template('upload_liste_payement.html',
                                   data_preview_html=None, uploaded_names=None,
                                   added_rows=0, duplicates_removed=0)

        filename = secure_filename(file.filename or "")
        ext = Path(filename).suffix.lower()
        if ext not in ('.xlsx', '.xls', '.csv'):
            flash("Format non supporté. Choisissez un fichier Excel (.xlsx/.xls) ou CSV (.csv).", "info")
            return render_template('upload_liste_payement.html',
                                   data_preview_html=None, uploaded_names=None,
                                   added_rows=0, duplicates_removed=0)
        try:
            required = {'RefContrat','DateCreation','DateReglement','Secteurs'}
            raw = file.read()

            
            if ext in ('.xlsx', '.xls'):
                df0 = pd.read_excel(BytesIO(raw), sheet_name=0, header=None, dtype=object)
                header_row = None
                for i in range(min(len(df0), 30)):
                    vals = {str(v).strip() for v in df0.iloc[i].tolist() if pd.notna(v)}
                    if required.issubset(vals):
                        header_row = i; break
                if header_row is not None:
                    header = [str(x).strip() if pd.notna(x) else '' for x in df0.iloc[header_row].tolist()]
                    df = df0.iloc[header_row+1:].copy(); df.columns = header
                    df = df.astype(str)
                else:
                    df = pd.read_excel(BytesIO(raw), sheet_name=0, dtype=str, keep_default_na=False)
                saver = ('excel', None, None)

            else:  # CSV
                text, enc_used = None, None
                for enc in ('utf-8-sig','utf-8','cp1252','latin-1'):
                    try:
                        text = raw.decode(enc); enc_used = enc; break
                    except UnicodeDecodeError:
                        continue
                if text is None:
                    text = raw.decode('utf-8', errors='replace'); enc_used = 'utf-8'

                sample = text[:10000]
                try:
                    dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
                    sep = dialect.delimiter
                except Exception:
                    sep = ','

                df0 = pd.read_csv(StringIO(text), header=None, dtype=object, sep=sep, engine='python')
                header_row = None
                for i in range(min(len(df0), 30)):
                    vals = {str(v).strip() for v in df0.iloc[i].tolist() if pd.notna(v)}
                    if required.issubset(vals):
                        header_row = i; break
                if header_row is not None:
                    df = pd.read_csv(StringIO(text), header=header_row, dtype=str,
                                     keep_default_na=False, sep=sep, engine='python')
                else:
                    df = pd.read_csv(StringIO(text), dtype=str, keep_default_na=False, sep=sep, engine='python')
                saver = ('csv', sep, enc_used)

           
            df.rename(columns=lambda c: str(c).strip(), inplace=True)
            for col in required:
                if col in df.columns: df[col] = df[col].astype(str)
            df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

         
            preview_df = df.iloc[:10, :15].copy()
            data_preview_html = preview_df.to_html(classes='table table-sm table-striped',
                                                   index=False, border=0)

           
            payment_folder = app.config.get('PAYMENT_FOLDER')
            if not payment_folder:
                raise RuntimeError("CONFIG: PAYMENT_FOLDER manquant dans app.config.")
            Path(payment_folder).mkdir(parents=True, exist_ok=True)
            save_path = os.path.join(payment_folder, filename)

            kind, sep, enc = saver
            if kind == 'excel':
                df.to_excel(save_path, index=False)
            else:
                enc_out = 'utf-8-sig' if enc and enc.startswith('utf-8') else (enc or 'utf-8-sig')
                df.to_csv(save_path, index=False, sep=sep, encoding=enc_out)

            uploaded_names = [filename]
            added_rows = len(df)
            flash(f"Fichier de paiements enregistré ({filename}, {added_rows} lignes).", "info")

        except Exception as e:
            flash(f'Erreur lors du traitement : {e}', "danger")

        return render_template('upload_liste_payement.html',
                               data_preview_html=data_preview_html,
                               uploaded_names=uploaded_names,
                               added_rows=added_rows,
                               duplicates_removed=duplicates_removed)

    return render_template('upload_liste_payement.html',
                           data_preview_html=None,
                           uploaded_names=None,
                           added_rows=0,
                           duplicates_removed=0)


def _compute_retablissemements(app):
    secteur = _active_secteur()
    if not secteur:
        raise ValueError("Secteur non sélectionné.")

    payment_folder = app.config['PAYMENT_FOLDER']
    if not os.path.isdir(payment_folder):
        raise FileNotFoundError("Le dossier de paiements n'existe pas.")

    pay = _load_all_payments(payment_folder)
    if pay.empty:
        raise ValueError("Aucun fichier de paiements (csv/xlsx) trouvé.")
  

  
    if 'Secteurs' not in pay.columns:
        raise ValueError("Les paiements ne contiennent pas la colonne « Secteurs ».")

    # filtrage sur le secteur actif
    pay['Secteurs'] = pay['Secteurs'].astype(str).str.strip()
    pay = pay[pay['Secteurs'] == secteur].copy()

    if pay.empty:
        raise ValueError(f"Aucun paiement pour le secteur « {secteur} ».")

    if 'RefContrat' in pay.columns:
        pay['RefContrat'] = pay['RefContrat'].astype(str).str.strip()
    else:
        pay['RefContrat'] = pd.NA

    if 'MontantReglement' not in pay.columns:
        pay['MontantReglement'] = '0'
    pay['MontantReglement_num'] = pay['MontantReglement'].apply(_to_number_cfa).fillna(0)
    
    def _pick_secteur(s: pd.Series) -> str:
        s = s.astype(str).str.strip().replace({'nan':'','NaN':'','None':'','NULL':''})
        s = s[s!='']
        if s.empty: return ''
        try:
            m = s.mode(dropna=True)
            if len(m)>0: return str(m.iloc[0])
        except Exception: pass
        return str(s.iloc[-1])

    secteur_by_ref = (
        pay.groupby('RefContrat', dropna=False, as_index=False)
           .agg(Secteur=('Secteurs', _pick_secteur))
    )

    pay_agg = (
        pay.groupby('RefContrat', dropna=False, as_index=False)
           .agg(TotalPayes=('MontantReglement_num', 'sum'))
           .dropna(subset=['RefContrat'])
    )

    # Impayés — SECTEUR
    imp_path = _impaye_path_for_sector(secteur)
    if not imp_path.exists():
        raise FileNotFoundError(
            f"Fichier des impayés introuvable"
        )
    imp = _load_impayes(str(imp_path))
    imp = clean_num_compteur(imp)

    want_cols = ['RefContrat','Telephone_prive','Telephone_pro',
                 'Total impayés échus en franc','Num_compteur']
    for c in want_cols:
        if c not in imp.columns: imp[c] = pd.NA

    imp['RefContrat'] = imp['RefContrat'].astype(str).str.strip()
    imp['Solde_num']  = imp['Total impayés échus en franc'].apply(_to_number_cfa).fillna(0)

    imp_agg = (
        imp.groupby('RefContrat', dropna=False, as_index=False)
           .agg({
               'Solde_num': 'first',
               'Total impayés échus en franc': 'first',
               'Num_compteur': 'first',
               'Telephone_prive': 'first',
               'Telephone_pro': 'first',
           })
    )
    imp_agg['Telephone_prive'] = imp_agg['Telephone_prive'].apply(normalize_phone_ci)
    imp_agg['Telephone_pro']   = imp_agg['Telephone_pro'].apply(normalize_phone_ci)
    imp_agg['Telephone_prive'] = (
        imp_agg['Telephone_prive'].fillna('') +
        imp_agg['Telephone_pro'].fillna('').apply(lambda x: f"<br>{x}" if x else '')
    )

    out_all = pay_agg.merge(
        imp_agg[['RefContrat','Solde_num','Total impayés échus en franc',
                 'Num_compteur','Telephone_prive']],
        on='RefContrat', how='left'
    ).merge(secteur_by_ref, on='RefContrat', how='left')

    out_all['Reste'] = (out_all['Solde_num'] - out_all['TotalPayes'])

    nb_clients_total   = pay['RefContrat'].astype(str).str.strip().replace('', pd.NA).dropna().nunique()
    out = out_all[out_all['Reste'].fillna(1) <= 0].copy()
    nb_eligibles_total = len(out)


    eligibles_by_zone = {secteur: nb_eligibles_total}

    stats = {
        'nb_clients_total':   int(nb_clients_total),
        'nb_eligibles_total': int(nb_eligibles_total),
        'total_payes_total':  int(out_all['TotalPayes'].sum(skipna=True)),
        'total_soldes_total': float(out_all['Solde_num'].sum(skipna=True)),
        'reste_total':        float(out_all['Reste'].sum(skipna=True)),
        'eligibles_by_zone':  eligibles_by_zone,
        'secteur_actif':      secteur,
        
    }

    display = out[['RefContrat','Num_compteur',
                   'Total impayés échus en franc','Secteur','Telephone_prive']].copy()
    display.sort_values(by=['RefContrat'], inplace=True, na_position='last')
    display.rename(columns={
        'Total impayés échus en franc': 'total_impaye_facture',
        'Telephone_prive': 'telephone'
    }, inplace=True)
    display = display[['RefContrat','Num_compteur','total_impaye_facture','Secteur','telephone']]

    return display, stats


@app.route('/retablissements', methods=['GET'])
@_require_secteur
def retablissements():
    try:
        secteur = _active_secteur()
        donnees_retabl, stats = _compute_retablissemements(app)

        colonnes  = ['RefContrat','Num_compteur','total_impaye_facture','Secteur','telephone']
        colonnes6 = colonnes + ['date_insertion']

        # nettoyage colonnes
        donnees_retabl = donnees_retabl.loc[:, [c for c in colonnes if c in donnees_retabl.columns]].copy()
        if 'RefContrat' in donnees_retabl.columns:
            donnees_retabl['RefContrat']   = donnees_retabl['RefContrat'].astype('string').map(_canon_key_str)
        if 'Num_compteur' in donnees_retabl.columns:
            donnees_retabl['Num_compteur'] = donnees_retabl['Num_compteur'].astype('string').map(_canon_key_str)
        if 'total_impaye_facture' in donnees_retabl.columns:
            donnees_retabl['total_impaye_facture'] = pd.to_numeric(
                donnees_retabl['total_impaye_facture'], errors='coerce'
            ).round(0).astype('Int64')
        if 'telephone' in donnees_retabl.columns:
            t = donnees_retabl['telephone']
            donnees_retabl['telephone'] = (
                t.where(t.notna(), '')
                 .replace(r'^\s*(nan|NaN|null|None)\s*$', '', regex=True)
                 .astype(str)
                 .str.replace(r'<br\s*/?>', ' ', regex=True)
                 .str.replace(r'\s+', ' ', regex=True)
                 .str.strip()
            )
        if 'Secteur' in donnees_retabl.columns:
            donnees_retabl['Secteur'] = donnees_retabl['Secteur'].fillna('').astype(str).str.strip()

        # déduplication
        cles = ['RefContrat','Num_compteur','total_impaye_facture']
        donnees_retabl = donnees_retabl.dropna(subset=['RefContrat','Num_compteur'])
        donnees_retabl = donnees_retabl[(donnees_retabl['RefContrat']!='') & (donnees_retabl['Num_compteur']!='')]
        donnees_retabl = donnees_retabl.drop_duplicates(subset=cles, keep='last')

        # fichier par secteur
        xlsx_path = _savebook_path_for_sector(secteur)

        if xlsx_path.exists():
            xls = pd.ExcelFile(xlsx_path)
            sheets = xls.sheet_names
            df_sav = pd.concat([xls.parse(s, dtype=str) for s in sheets],
                               ignore_index=True) if sheets else pd.DataFrame(columns=colonnes6)
            lots_existants = []
            for s in sheets:
                m = re.match(r'^\s*lot\s+(\d+)\s*$', s, flags=re.IGNORECASE)
                if m: lots_existants.append(int(m.group(1)))
            last_lot_num   = max(lots_existants) if lots_existants else 0
            existing_sheets = set(sheets)
        else:
            df_sav = pd.DataFrame(columns=colonnes6)
            last_lot_num = 0
            existing_sheets = set()

        for c in colonnes6:
            if c not in df_sav.columns:
                df_sav[c] = pd.Series(dtype='object')

      
        df_sav['RefContrat']   = df_sav['RefContrat'].astype('string').map(_canon_key_str)
        df_sav['Num_compteur'] = df_sav['Num_compteur'].astype('string').map(_canon_key_str)
        df_sav['total_impaye_facture'] = pd.to_numeric(df_sav['total_impaye_facture'], errors='coerce').round(0).astype('Int64')
        if 'telephone' in df_sav.columns:
            t = df_sav['telephone']
            df_sav['telephone'] = (
                t.where(t.notna(), '')
                 .replace(r'^\s*(nan|NaN|null|None)\s*$', '', regex=True)
                 .astype(str)
                 .str.replace(r'<br\s*/?>', ' ', regex=True)
                 .str.replace(r'\s+', ' ', regex=True)
                 .str.strip()
            )
        if 'Secteur' in df_sav.columns:
            df_sav['Secteur'] = df_sav['Secteur'].fillna('').astype(str).str.strip()

        df_sav = df_sav.dropna(subset=['RefContrat','Num_compteur'])
        df_sav = df_sav.drop_duplicates(subset=cles, keep='last')

        base_cle = df_sav[cles].drop_duplicates() if not df_sav.empty else pd.DataFrame(columns=cles)
        fusion   = donnees_retabl.merge(base_cle, how='left', on=cles, indicator=True)
        nouveaux = fusion[fusion['_merge']=='left_only'].drop(columns=['_merge'])
        nb_nouveaux = len(nouveaux)

        if nb_nouveaux > 0:
            next_lot   = last_lot_num + 1
            sheet_name = f"lot {next_lot}"
            while sheet_name in existing_sheets:
                next_lot += 1
                sheet_name = f"lot {next_lot}"

            to_write = nouveaux[colonnes].copy()
            to_write['telephone'] = (
                to_write['telephone'].fillna('')
                      .astype(str)
                      .str.replace(r'<br\s*/?>', ' ', regex=True)
                      .str.replace(r'\s+', ' ', regex=True)
                      .str.strip()
            )
            to_write['Secteur'] = to_write['Secteur'].fillna('').astype(str).str.strip()
            to_write['date_insertion'] = _tz_now().strftime('%Y-%m-%d %H:%M:%S')

            mode = 'a' if xlsx_path.exists() else 'w'
            with pd.ExcelWriter(xlsx_path, engine='openpyxl', mode=mode) as w:
                to_write[colonnes6].to_excel(w, sheet_name=sheet_name, index=False)

            flash(f"{nb_nouveaux} nouveau(x) client(s) ajouté(s) dans (secteur {secteur}).", "success")
            table_source = to_write[colonnes6].copy()
        else:
            if last_lot_num > 0 and xlsx_path.exists():
                last_sheet   = f"lot {last_lot_num}"
                table_source = pd.read_excel(xlsx_path, sheet_name=last_sheet, dtype=str)
                if 'total_impaye_facture' in table_source.columns:
                    table_source['total_impaye_facture'] = pd.to_numeric(
                        table_source['total_impaye_facture'], errors='coerce'
                    ).round(0).astype('Int64')
                if 'telephone' in table_source.columns:
                    t = table_source['telephone']
                    table_source['telephone'] = (
                        t.where(t.notna(), '')
                         .replace(r'^\s*(nan|NaN|null|None)\s*$', '', regex=True)
                         .astype(str)
                         .str.replace(r'<br\s*/?>', ' ', regex=True)
                         .str.replace(r'\s+', ' ', regex=True)
                         .str.strip()
                    )
                if 'Secteur' in table_source.columns:
                    table_source['Secteur'] = table_source['Secteur'].fillna('').astype(str).str.strip()
                table_source = table_source.loc[:, [c for c in colonnes6 if c in table_source.columns]]
                flash("Aucun nouveau client ", "info")
            else:
                table_source = pd.DataFrame(columns=colonnes6)
                flash("Aucun nouveau client", "info")

        if 'total_impaye_facture' in table_source.columns:
                 table_source['total_impaye_facture'] = pd.to_numeric(
                table_source['total_impaye_facture'], errors='coerce'
            ).fillna(0).astype(int).map("{:,}".format)

        table_html = table_source.to_html(
            classes='table table-sm table-striped table-hover align-left',
            index=False, border=0, table_id='table-retab', escape=False, na_rep=''
        )
        return render_template('retablissements.html', table_html=table_html, stats=stats)
    except Exception as e:
        flash(f"Erreur pendant le calcul : {e}", "danger")
        return render_template('retablissements.html', table_html=None, stats=None)


@app.route('/retablissements/download_excel', methods=['GET'])
@_require_secteur
def download_retab_excel():
    try:
        secteur = _active_secteur()
        src_file = _savebook_path_for_sector(secteur)

        if not src_file.exists():
            flash("Fichierintrouvable pour ce secteur. Ouvrez d’abord la page « Clients éligibles ».","warning")
            return redirect(url_for('retablissements'))


        sheets = pd.read_excel(src_file, sheet_name=None, dtype=str)
        cleaned = {name: _clean_telephone_col(df.copy()) for name, df in sheets.items()}

        buf = BytesIO()
        with pd.ExcelWriter(buf, engine='openpyxl') as writer:
            for name, df in cleaned.items():
                df.to_excel(writer, sheet_name=name, index=False)
        buf.seek(0)

        stamp = _tz_now().strftime('%Y-%m-%d_%H-%M')
        download_name = f"clients_eligibles_{secteur}_{stamp}.xlsx"

        return send_file(
            buf, as_attachment=True, download_name=download_name,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    except Exception as e:
        flash(f"Export Excel impossible : {e}", "danger")
        return redirect(url_for('retablissements'))

@app.route('/retablissements/download_pdf', methods=['GET'])
@_require_secteur
def download_retab_pdf():
    try:
        secteur = _active_secteur()
        xlsx_path = _savebook_path_for_sector(secteur)

        if xlsx_path.exists():
            sheets = pd.read_excel(xlsx_path, sheet_name=None, dtype=str)
        else:

            df, _ = _compute_retablissemements(app)
            df['date_insertion'] = _tz_now().strftime('%Y-%m-%d %H:%M:%S')
            sheets = {'clients_eligibles': df}

        def _build_out(df):
            mapping = {
                'RefContrat': ['RefContrat', 'ref_contrat'],
                'Num_compteur': ['Num_compteur','num_compteur','Compteur'],
                'total_impaye_facture': ['total_impaye_facture','Total impayés échus en franc','Solde_num','impaye_fa'],
                'telephone': ['telephone','Telephone','Telephone_prive','tel','Téléphone'],
                'Secteur': ['Secteur','Secteurs'],
                'date_insertion': ['date_insertion','Date_insertion','date']
            }
            res = {}
            for tgt, cands in mapping.items():
                for c in cands:
                    if c in df.columns:
                        res[tgt] = c; break

            out = pd.DataFrame()
            out['RefContrat']   = df.get(res.get('RefContrat','RefContrat'), '').astype(str).map(_canon_key_str).fillna('')
            out['Num_compteur'] = df.get(res.get('Num_compteur','Num_compteur'), '').astype(str).map(_canon_key_str).fillna('')
            amt = pd.to_numeric(df.get(res.get('total_impaye_facture','total_impaye_facture'), ''), errors='coerce')
            out['total_impaye_facture'] = amt.round(0).astype('Int64')
            tel = df.get(res.get('telephone','telephone'), '')
            out['telephone'] = (
                pd.Series(tel).where(pd.Series(tel).notna(), '')
                .astype(str).replace(r'^\s*(nan|NaN|null|None)\s*$', '', regex=True)
                .str.replace(r'<br\s*/?>', ' ', regex=True).str.replace(r'\s+', ' ', regex=True).str.strip()
            )
            out['Secteur'] = df.get(res.get('Secteur','Secteur'), secteur)
            if 'date_insertion' in res:
                di = df[res['date_insertion']]
                out['date_insertion'] = (
                    di.where(di.notna(), '')
                      .astype(str).replace(r'^\s*(nan|NaN|null|None)\s*$', '', regex=True)
                )
            else:
                out['date_insertion'] = _tz_now().strftime('%Y-%m-%d %H:%M:%S')

            return out[['RefContrat','Num_compteur','total_impaye_facture','Secteur','telephone','date_insertion']]

        frames = [_build_out(df) for _, df in sheets.items()]
        out = pd.concat(frames, ignore_index=True)

        out_pdf = out.fillna('')
        out_pdf['total_impaye_facture'] = out_pdf['total_impaye_facture'].astype('Int64').astype(str).replace('<NA>', '')

        pdf_folder = _savebook_path_for_sector(secteur).parent
        pdf_path = pdf_folder / "clients_eligibles.pdf"

        styles = getSampleStyleSheet()
        doc = SimpleDocTemplate(
            str(pdf_path), pagesize=landscape(A4),
            leftMargin=18, rightMargin=18, topMargin=18, bottomMargin=18
        )

        data = [list(out_pdf.columns)] + out_pdf.astype(str).values.tolist()
        tbl = Table(data, repeatRows=1)
        tbl.setStyle(TableStyle([
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("BACKGROUND", (0, 0), (-1, 0), colors.whitesmoke),
            ("ALIGN", (0, 0), (-1, -1), "LEFT"),
            ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F7F7F7")]),
            ("FONTSIZE", (0, 0), (-1, -1), 9),
            ("LEFTPADDING", (0, 0), (-1, -1), 6),
            ("RIGHTPADDING", (0, 0), (-1, -1), 6),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ]))

        ts_hdr = _tz_now().strftime('%Y-%m-%d %H:%M:%S')
        story = [
            Paragraph(f"Clients éligibles — secteur {secteur} — {len(sheets)} feuille(s), {len(out_pdf)} lignes — export {ts_hdr}", styles["Heading2"]),
            Spacer(1, 8),
            tbl
        ]
        doc.build(story)

        stamp = _tz_now().strftime('%Y-%m-%d_%H-%M')
        download_name = f"clients_eligibles_{secteur}_{stamp}.pdf"
        return send_file(str(pdf_path), as_attachment=True, download_name=download_name)
    except Exception as e:
        flash(f"Export PDF impossible : {e}", "danger")
        return redirect(url_for('retablissements'))


# Gestion fichiers paiements 

def _list_payment_files():
    folder = Path(app.config['PAYMENT_FOLDER']).resolve()
    folder.mkdir(parents=True, exist_ok=True)
    files = []
    for p in folder.iterdir():
        if p.is_file() and p.suffix.lower() in ALLOWED_PAYMENT_EXTS:
            st = p.stat()
            files.append({
                "name": p.name,
                "size": _human_size(st.st_size),
                "mtime": datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M"),
            })
    files.sort(key=lambda x: x["mtime"], reverse=True)
    return files

@app.route('/paiements_fichiers', methods=['GET', 'POST'])
def paiements_fichiers():
    payment_dir = Path(app.config['PAYMENT_FOLDER']).resolve()
    payment_dir.mkdir(parents=True, exist_ok=True)

    if request.method == 'POST':
        filenames = request.form.getlist('filenames')
        if not filenames:
            flash("Aucun fichier sélectionné.", "info")
            # PRG : 303 vers GET (URL unique avec t=… pour éviter tout replay de cache)
            return _no_store(_see_other('paiements_fichiers', t=int(time())))

        deleted, skipped = [], []
        for filename in filenames:
            safe_name = os.path.basename(filename).strip()
            candidate = (payment_dir / safe_name).resolve()

            # garde anti-traversal
            if payment_dir not in candidate.parents and candidate != payment_dir:
                skipped.append(safe_name)
                continue

            # whitelist d’extensions
            if candidate.suffix.lower() not in ALLOWED_PAYMENT_EXTS:
                skipped.append(safe_name)
                continue

            try:
                if candidate.exists() and candidate.is_file():
                    candidate.unlink()
                    deleted.append(safe_name)
                else:
                    skipped.append(safe_name)
            except Exception as e:
                skipped.append(f"{safe_name} (err: {e})")

        if deleted:
            flash(f"Supprimés : {', '.join(deleted)}", "success")
        if skipped:
            flash(f"Non supprimés : {', '.join(skipped)}", "warning")

        # PRG : retour 303 vers GET
        return _no_store(_see_other('paiements_fichiers', t=int(time())))

    # -------- GET : unique rendu du template (pas de redirection ici) --------
    files = _list_payment_files()

    resp = make_response(render_template('paiements_fichiers.html', files=files), 200)
    return _no_store(resp)

def nettoyer_sauvegardes():
    root = Path(app.config['SAVEPAYMENT_FOLDER']).resolve()
    if root.exists():
        for f in root.rglob('*'):
            if f.is_file():
                try:
                    os.remove(f)
                    app.logger.info(f"Supprimé : {f}")
                except Exception as e:
                    app.logger.error(f"Erreur suppression {f} : {e}")

scheduler = BackgroundScheduler()
scheduler.add_job(nettoyer_sauvegardes, 'cron', hour=2, minute=0)
scheduler.start()
logging.getLogger('apscheduler').setLevel(logging.WARNING)


def _dossier_sauvegarde() -> Path:
    dossier = Path(app.config['SAVEPAYMENT_FOLDER']).resolve()
    dossier.mkdir(parents=True, exist_ok=True)
    return dossier

def _verifier_chemin_secure(nom_fichier: str) -> Path:
    dossier = _dossier_sauvegarde()
    nom_nettoye = secure_filename(nom_fichier)
    if not nom_nettoye:
        raise ValueError("Nom de fichier invalide.")
    chemin = (dossier / nom_nettoye).resolve()
    if dossier not in chemin.parents and chemin != dossier:
        raise ValueError("Chemin de fichier non autorisé.")
    return chemin

@app.route('/sauvegardes', methods=['GET'])
def lister_sauvegardes():
    try:
        dossier = _dossier_sauvegarde()
        fichiers = []
        for f in sorted(dossier.rglob('*')):
            if f.is_file():
                stat = f.stat()
                fichiers.append({
                    "nom": str(f.relative_to(dossier)),
                    "taille": stat.st_size,
                    "modifie_le": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
                })
        return render_template('sauvegardes.html', fichiers=fichiers)
    except Exception as e:
        flash(f"Erreur lors de la lecture des sauvegardes : {e}", "danger")
        return render_template('sauvegardes.html', fichiers=[])

@app.route('/sauvegardes/telecharger/<path:nom_fichier>', methods=['GET'])
def telecharger_sauvegarde(nom_fichier: str):
    try:
       
        base = _dossier_sauvegarde()
        chemin = (base / nom_fichier).resolve()
        if base not in chemin.parents and chemin != base:
            raise ValueError("Chemin non autorisé.")
        if not chemin.exists():
            raise FileNotFoundError("Fichier introuvable.")
        return send_from_directory(directory=str(chemin.parent), path=chemin.name, as_attachment=True)
    except Exception as e:
        flash(f"Téléchargement impossible : {e}", "danger")
        return redirect(url_for('lister_sauvegardes'))

@app.route('/sauvegardes/supprimer', methods=['POST'])
def supprimer_sauvegarde():
    nom_fichier = request.form.get('nom_fichier', '')
    try:
        base = _dossier_sauvegarde()
        chemin = (base / nom_fichier).resolve()
        if base not in chemin.parents and chemin != base:
            raise ValueError("Chemin non autorisé.")
        if not chemin.exists() or not chemin.is_file():
            flash("Fichier introuvable.", "warning")
            return redirect(url_for('lister_sauvegardes'))
        os.remove(chemin)
        flash(f"Fichier « {chemin.name} » supprimé.", "success")
    except Exception as e:
        flash(f"Suppression impossible : {e}", "danger")
    return redirect(url_for('lister_sauvegardes'))


@app.route('/diag_fs')
def diag_fs():
    import os, getpass, platform
    from pathlib import Path
    import traceback
    try:
        uid = getattr(os, "geteuid", lambda: "N/A")()
        user = getpass.getuser()
    except Exception:
        uid, user = "N/A", "N/A"

    dirs = {
        "PAYMENT_FOLDER": Path(app.config.get("PAYMENT_FOLDER", "")),
        "DATA_DIR": Path(app.config.get("DATA_DIR", "")),
        "instance_path": Path(app.instance_path),
    }
    report = {
        "platform": platform.platform(),
        "process_user": {"uid": str(uid), "name": user},
        "dirs": {},
    }

    for key, path in dirs.items():
        info = {"path": str(path.resolve()), "exists": path.exists()}
        try:
            path.mkdir(parents=True, exist_ok=True)
            info["w_ok"] = os.access(path, os.W_OK)
            info["x_ok"] = os.access(path, os.X_OK)
            # test write/delete
            test = path / ".__write_test__.txt"
            test.write_text("ok", encoding="utf-8")
            info["write"] = "ok"
            test.unlink()
            info["delete"] = "ok"
        except Exception as e:
            info["error"] = f"{type(e).__name__}: {e}"
            info["trace"] = traceback.format_exc().splitlines()[-1]
        report["dirs"][key] = info

    return report, 200


# ----------- Déconnexion -----------
@app.route('/logout', methods=['GET'])
def logout():
    session.clear()  
    flash("Vous êtes déconnecté.", "info")
    return redirect(url_for('choisir_secteur'))  

# -------------------------------------------------------
if __name__ == '__main__':
    app.run(debug=True, use_reloader=False, host='0.0.0.0', port=5003)




 


