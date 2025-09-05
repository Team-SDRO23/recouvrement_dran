
from flask import Flask, render_template, request, redirect, url_for, session, flash,send_file,send_from_directory
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
import os, io,csv, re
from io import BytesIO,StringIO
import numpy as np
import pandas as pd
from flask_sqlalchemy import SQLAlchemy
from pathlib import Path
from datetime import datetime
from utils import _read_any_file,_to_number_cfa,_load_all_payments,_load_impayes,normalize_phone_ci,clean_num_compteur,_human_size,_canon_key_str
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import getSampleStyleSheet
import shutil
from apscheduler.schedulers.background import BackgroundScheduler
import logging



from flask import abort  
ALLOWED_PAYMENT_EXTS = {'.xlsx', '.xls', '.csv'}  






app = Flask(__name__)
app.secret_key = 'secret_key'
app.config['UPLOAD_FOLDER'] = 'impayefacture'
app.config['PAYMENT_FOLDER'] = 'payementfacture'
app.config['SAVEPAYMENT_FOLDER']='sauvegardepayement'
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['PAYMENT_FOLDER'], exist_ok=True)
os.makedirs(app.config['SAVEPAYMENT_FOLDER'], exist_ok=True)

app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///users.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)


# ----------- Page d'accueil -----------
@app.route('/')
def home():

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
    # Repli : ligne la plus dense
    non_na_counts = df0.notna().sum(axis=1)
    return int(non_na_counts.idxmax())


@app.route('/upload_liste_impaye', methods=['GET','POST'])
def upload_liste_impaye():

    preview_html = None
    orig_filename = None

    if request.method == 'POST':
        file = request.files.get('file')
        if not file or not file.filename:
            flash("Veuillez choisir un fichier.")
            return render_template('upload_liste_impaye.html')

        ext = file.filename.lower().rsplit('.', 1)[-1]
        if ext not in ('xlsx', 'xls', 'csv'):
            flash("Format non supporté. Choisissez un fichier Excel (.xlsx/.xls) ou CSV.")
            return render_template('upload_liste_impaye.html')

        try:
            orig_filename = secure_filename(file.filename)


            raw = file.read()
            bio = BytesIO(raw)

           
            if ext == 'csv':
                df0 = pd.read_csv(bio, header=None, dtype=object)
            else:
                df0 = pd.read_excel(bio, sheet_name=0, header=None, dtype=object)

            required = [
                'Matricule AZ', 'Nom AZ', 'Tournee','Genre client'
            ]
            header_row = find_header_row(df0, required_labels=required, min_match=3)

            header = [str(x).strip() if pd.notna(x) else "" for x in df0.iloc[header_row].tolist()]
            df = df0.iloc[header_row+1:].copy()
            df.columns = header

            df = df.replace(r'^\s*$', pd.NA, regex=True)

           
            colnames = list(df.columns)
            mask_bad = np.array([
                (c is None) or (str(c).strip() == "") or str(c).lower().startswith("unnamed")
                for c in colnames
            ], dtype=bool)
            if mask_bad.any():
                df = df.iloc[:, ~mask_bad]

          
            df = df.dropna(axis=1, how='all')
            df = df.dropna(axis=0, how='all')

            df.columns = [str(c).strip() for c in df.columns]
            df = df.loc[:, ~pd.Index(df.columns).duplicated()]
    

            os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
            save_path = os.path.join(app.config['UPLOAD_FOLDER'], 'factureimpaye.xlsx')
            df.to_excel(save_path, index=False)


            df_preview = df.iloc[:, :5].head(10).fillna("")
            preview_html = df_preview.to_html(
                classes="table table-sm table-striped table-bordered align-middle",
                index=False,
                border=0,
                escape=False
            )

            flash("Fichier chargé et traité avec succès.")

        except Exception as e:
            flash(f"Erreur lors du traitement du fichier : {e}")

    return render_template('upload_liste_impaye.html',
                           preview_html=preview_html,
                           orig_filename=orig_filename)



@app.route('/upload_liste_payement', methods=['GET','POST'])
def upload_liste_payement():

    data_preview_html = None
    uploaded_names = []
    added_rows = 0
    duplicates_removed = 0  

    if request.method == 'POST':
        
        file = request.files.get('file')
      
        if file is None:
            files_list = request.files.getlist('files')
            file = files_list[0] if files_list else None

        if not file:
            flash("Veuillez choisir un fichier (.xlsx, .xls ou .csv).")
            return render_template('upload_liste_payement.html',
                                   data_preview_html=None, uploaded_names=None,
                                   added_rows=0, duplicates_removed=0)

        filename = secure_filename(file.filename or "")
        ext = Path(filename).suffix.lower()
        if ext not in ('.xlsx', '.xls', '.csv'):
            flash("Format non supporté. Choisissez un fichier Excel (.xlsx/.xls) ou CSV (.csv).")
            return render_template('upload_liste_payement.html',
                                   data_preview_html=None, uploaded_names=None,
                                   added_rows=0, duplicates_removed=0)

        try:
            required = {'RefContrat', 'DateCreation', 'DateReglement', 'Secteurs'}
            raw = file.read()

       
            if ext in ('.xlsx', '.xls'):
              
                df0 = pd.read_excel(BytesIO(raw), sheet_name=0, header=None, dtype=object)
                header_row = None
                for i in range(min(len(df0), 30)):  
                    vals = {str(v).strip() for v in df0.iloc[i].tolist() if pd.notna(v)}
                    if required.issubset(vals):
                        header_row = i
                        break

                if header_row is not None:
                    header = [str(x).strip() if pd.notna(x) else '' for x in df0.iloc[header_row].tolist()]
                    df = df0.iloc[header_row+1:].copy()
                    df.columns = header
                
                    df = df.astype(str)
                else:
                   
                    df = pd.read_excel(BytesIO(raw), sheet_name=0, dtype=str, keep_default_na=False)

               
                saver = 'excel'

            else: 

                text = None
                encoding_used = None
                for enc in ('utf-8-sig', 'utf-8', 'cp1252', 'latin-1'):
                    try:
                        text = raw.decode(enc)
                        encoding_used = enc
                        break
                    except UnicodeDecodeError:
                        continue
                if text is None:
                    text = raw.decode('utf-8', errors='replace')
                    encoding_used = 'utf-8'

               
                sample = text[:10000]
                try:
                    dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
                    sep = dialect.delimiter
                except Exception:
                    sep = ','

                df0 = pd.read_csv(StringIO(text), header=None, dtype=object,
                                  sep=sep, engine='python')
                header_row = None
                for i in range(min(len(df0), 30)):
                    vals = {str(v).strip() for v in df0.iloc[i].tolist() if pd.notna(v)}
                    if required.issubset(vals):
                        header_row = i
                        break

                if header_row is not None:
                    df = pd.read_csv(StringIO(text), header=header_row, dtype=str,
                                     keep_default_na=False, sep=sep, engine='python')
                else:
                    df = pd.read_csv(StringIO(text), dtype=str,
                                     keep_default_na=False, sep=sep, engine='python')


                saver = ('csv', sep, encoding_used)

        
            df.rename(columns=lambda c: str(c).strip(), inplace=True)

            
            for col in required:
                if col in df.columns:
                    df[col] = df[col].astype(str)

            df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

            preview_df = df.iloc[:10, :15].copy()
            data_preview_html = preview_df.to_html(classes='table table-sm table-striped',
                                                   index=False, border=0)

            payment_folder = app.config['PAYMENT_FOLDER']
            os.makedirs(payment_folder, exist_ok=True)

            save_path = os.path.join(payment_folder, filename)

            if saver == 'excel':
                df.to_excel(save_path, index=False)
            else:
                _, sep, enc = saver
                
                enc_out = 'utf-8-sig' if enc.startswith('utf-8') else enc
                df.to_csv(save_path, index=False, sep=sep, encoding=enc_out)

            uploaded_names = [filename]
            added_rows = len(df)
            flash(f"Fichier traité et enregistré : {filename} ({added_rows} lignes).")

        except Exception as e:
            flash(f'Erreur lors du traitement : {e}')

        return render_template('upload_liste_payement.html',
                               data_preview_html=data_preview_html,
                               uploaded_names=uploaded_names,
                               added_rows=added_rows,
                               duplicates_removed=duplicates_removed)

    # GET
    return render_template('upload_liste_payement.html',
                           data_preview_html=None,
                           uploaded_names=None,
                           added_rows=0,
                           duplicates_removed=0)

def _compute_retablissemements(app):

    payment_folder = app.config['PAYMENT_FOLDER']
    if not os.path.isdir(payment_folder):
        raise FileNotFoundError("Le dossier de paiements n'existe pas.")

    pay = _load_all_payments(payment_folder)
    if pay.empty:
        raise ValueError("Aucun fichier de paiements (csv/xlsx) trouvé dans le dossier.")

    
    if 'RefContrat' in pay.columns:
        pay['RefContrat'] = pay['RefContrat'].astype(str).str.strip()
    else:
        pay['RefContrat'] = pd.NA


    if 'MontantReglement' not in pay.columns:
        pay['MontantReglement'] = '0'

    pay['MontantReglement_num'] = pay['MontantReglement'].apply(_to_number_cfa).fillna(0)

  
    pay_agg = (
        pay.groupby('RefContrat', dropna=False, as_index=False)
           .agg(TotalPayes=('MontantReglement_num', 'sum'))
           .dropna(subset=['RefContrat'])
    )


    imp_path = os.path.join(app.config['UPLOAD_FOLDER'], 'factureimpaye.xlsx')
    if not os.path.exists(imp_path):
        raise FileNotFoundError("Fichier des impayés introuvable (factureimpaye.xlsx).")

    imp = _load_impayes(imp_path)
    
    imp = clean_num_compteur(imp)


    want_cols = ['RefContrat', 'Telephone_prive', 'Telephone_pro', 'Total impayés échus en franc', 'Num_compteur']
    for c in want_cols:
        if c not in imp.columns:
            imp[c] = pd.NA

    imp['RefContrat'] = imp['RefContrat'].astype(str).str.strip()
    imp['Solde_num'] = imp['Total impayés échus en franc'].apply(_to_number_cfa).fillna(0)

  
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
    imp_agg['Telephone_pro'] = imp_agg['Telephone_pro'].apply(normalize_phone_ci)
    imp_agg['Telephone_prive'] = (
        imp_agg['Telephone_prive'].fillna('') +
        imp_agg['Telephone_pro'].fillna('').apply(lambda x: f"<br>{x}" if x else '')
    )

  
    out_all = pay_agg.merge(
        imp_agg[['RefContrat', 'Solde_num', 'Total impayés échus en franc', 'Num_compteur', 'Telephone_prive']],
        on='RefContrat',
        how='left'
    )

 
    out_all['Reste'] = (out_all['Solde_num'] - out_all['TotalPayes'])

  
    nb_clients_total = (
        pay['RefContrat'].astype(str).str.strip().replace('', pd.NA).dropna().nunique()
    )
    nb_eligibles_total = (out_all['Reste'] <= 0).fillna(False).sum()

    stats = {
        'nb_clients_total':   int(nb_clients_total),
        'nb_eligibles_total': int(nb_eligibles_total),
        'total_payes_total':  float(out_all['TotalPayes'].sum(skipna=True)),
        'total_soldes_total': float(out_all['Solde_num'].sum(skipna=True)),
        'reste_total':        float(out_all['Reste'].sum(skipna=True)),
    }


    out = out_all[out_all['Reste'].fillna(1) <= 0].copy()


    display = out[['RefContrat', 'Num_compteur', 'Total impayés échus en franc', 'Telephone_prive']].copy()
    display.sort_values(by=['RefContrat'], inplace=True, na_position='last')

    display.rename(columns={
       
        'Total impayés échus en franc': 'total_impaye_facture',
        'Telephone_prive': 'telephone'
    }, inplace=True)

    display = display[['RefContrat', 'Num_compteur', 'total_impaye_facture', 'telephone']]

    return display, stats


@app.route('/retablissements', methods=['GET'])
def retablissements():
    try:
        donnees_retabl, stats = _compute_retablissemements(app)

        colonnes = ['RefContrat', 'Num_compteur', 'total_impaye_facture', 'telephone']
        COL_LOT, COL_DATE = 'lot_id', 'date_insertion'

        donnees_retabl = donnees_retabl.loc[:, [c for c in colonnes if c in donnees_retabl.columns]].copy()
        if 'RefContrat' in donnees_retabl.columns:
            donnees_retabl['RefContrat'] = donnees_retabl['RefContrat'].astype('string').map(_canon_key_str)
        if 'Num_compteur' in donnees_retabl.columns:
            donnees_retabl['Num_compteur'] = donnees_retabl['Num_compteur'].astype('string').map(_canon_key_str)
        if 'total_impaye_facture' in donnees_retabl.columns:
            donnees_retabl['total_impaye_facture'] = pd.to_numeric(donnees_retabl['total_impaye_facture'], errors='coerce').round(0).astype('Int64')
        if 'telephone' in donnees_retabl.columns:
            t = donnees_retabl['telephone']
            donnees_retabl['telephone'] = (
                t.where(t.notna(), '')
                 .replace(r'^\s*(nan|NaN|null|None)\s*$', '', regex=True)
                 .astype(str).str.strip()
            )

        donnees_retabl = donnees_retabl.dropna(subset=['RefContrat','Num_compteur'])
        donnees_retabl = donnees_retabl[(donnees_retabl['RefContrat']!='') & (donnees_retabl['Num_compteur']!='')]

        cles = ['RefContrat','Num_compteur','total_impaye_facture']
        donnees_retabl = donnees_retabl.drop_duplicates(subset=cles, keep='last')

        dossier = Path(app.config['SAVEPAYMENT_FOLDER']).resolve()
        dossier.mkdir(parents=True, exist_ok=True)
        chemin = dossier / "save_payement.xlsx"

        try:
            df_sav = pd.read_excel(chemin, dtype=str)
        except FileNotFoundError:
            df_sav = pd.DataFrame(columns=colonnes + [COL_LOT, COL_DATE])

        for c in colonnes + [COL_LOT, COL_DATE]:
            if c not in df_sav.columns:
                df_sav[c] = pd.Series(dtype='object')

        df_sav['RefContrat'] = df_sav['RefContrat'].astype('string').map(_canon_key_str)
        df_sav['Num_compteur'] = df_sav['Num_compteur'].astype('string').map(_canon_key_str)
        df_sav['total_impaye_facture'] = pd.to_numeric(df_sav['total_impaye_facture'], errors='coerce').round(0).astype('Int64')
        if 'telephone' in df_sav.columns:
            t = df_sav['telephone']
            df_sav['telephone'] = (
                t.where(t.notna(), '')
                 .replace(r'^\s*(nan|NaN|null|None)\s*$', '', regex=True)
                 .astype(str).str.strip()
            )
        df_sav[COL_LOT] = pd.to_numeric(df_sav[COL_LOT], errors='coerce').astype('Int64')
        df_sav[COL_DATE] = pd.to_datetime(df_sav[COL_DATE], errors='coerce')
        df_sav = df_sav.dropna(subset=['RefContrat','Num_compteur'])
        df_sav = df_sav.drop_duplicates(subset=cles, keep='last')

        base_cle = df_sav[cles].drop_duplicates()
        fusion = donnees_retabl.merge(base_cle, how='left', on=cles, indicator=True)
        nouveaux = fusion[fusion['_merge']=='left_only'].drop(columns=['_merge'])
        nb_nouveaux = len(nouveaux)

        if nb_nouveaux > 0:
            last_lot = (df_sav[COL_LOT].dropna().max() if not df_sav.empty else pd.NA)
            next_lot = (int(last_lot)+1) if pd.notna(last_lot) else 1
            ts_now = pd.Timestamp(datetime.now())
            nouveaux = nouveaux.copy()
            nouveaux[COL_LOT] = next_lot
            nouveaux[COL_DATE] = ts_now
            df_out = pd.concat(
                [df_sav[colonnes+[COL_LOT,COL_DATE]],
                 nouveaux[colonnes+[COL_LOT,COL_DATE]]],
                ignore_index=True
            )
            df_out = df_out.drop_duplicates(subset=cles, keep='last')
            if 'telephone' in df_out.columns:
                df_out['telephone'] = df_out['telephone'].fillna('')
            with pd.ExcelWriter(chemin, engine='openpyxl', mode='w') as w:
                df_out.to_excel(w, index=False)
            flash(f"{nb_nouveaux} nouveau(x) client(s)", category='success')
            table_source = nouveaux[colonnes].copy()
        else:
            df_sav = df_sav.drop_duplicates(subset=cles, keep='last')
            if 'telephone' in df_sav.columns:
                df_sav['telephone'] = df_sav['telephone'].fillna('')
            with pd.ExcelWriter(chemin, engine='openpyxl', mode='w') as w:
                df_sav.to_excel(w, index=False)
            if not df_sav.empty and df_sav[COL_LOT].notna().any():
                last_lot = int(df_sav[COL_LOT].dropna().max())
                table_source = df_sav[df_sav[COL_LOT]==last_lot][colonnes].copy()
                
            else:
                table_source = pd.DataFrame(columns=colonnes)
        
       
        table_html = table_source.to_html(
            classes='table table-sm table-striped table-hover align-left',
            index=False, border=0, table_id='table-retab', escape=False, na_rep=''
        )
        return render_template('retablissements.html', table_html=table_html, stats=stats)
    except Exception as e:
        flash(f"Erreur pendant le calcul : {e}", category='danger')
        return render_template('retablissements.html', table_html=None, stats=None)


@app.route('/retablissements/download_excel', methods=['GET'])
def download_retab_excel():

    try:
        display, _ = _compute_retablissemements(app)
        out_file = os.path.join(app.config['UPLOAD_FOLDER'], "retablissements.xlsx")
        display.to_excel(out_file, index=False)
        return send_file(out_file, as_attachment=True, download_name="retablissements.xlsx")
    except Exception as e:
        flash(f"Export Excel impossible : {e}")
        return redirect(url_for('retablissements'))


@app.route('/retablissements/download_pdf', methods=['GET'])
def download_retab_pdf():
   
    try:
        display, _ = _compute_retablissemements(app)

        out_file = os.path.join(app.config['UPLOAD_FOLDER'], "retablissements.pdf")

        # PDF simple avec ReportLab
        styles = getSampleStyleSheet()
        doc = SimpleDocTemplate(out_file, pagesize=landscape(A4), leftMargin=18, rightMargin=18, topMargin=18, bottomMargin=18)

        data = [display.columns.tolist()] + display.astype(str).values.tolist()
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

        story = [
            Paragraph("Clients éligibles au rétablissement", styles["Heading2"]),
            Spacer(1, 8),
            tbl
        ]
        doc.build(story)

        return send_file(out_file, as_attachment=True, download_name="retablissements.pdf")
    except Exception as e:
        flash(f"Export PDF impossible : {e}")
        return redirect(url_for('retablissements'))



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
            flash("Aucun fichier sélectionné.")
            return redirect(url_for('paiements_fichiers'))

        deleted, skipped = [], []

        for filename in filenames:
            safe_name = os.path.basename(filename).strip()
            candidate = (payment_dir / safe_name).resolve()

            if payment_dir not in candidate.parents and candidate != payment_dir:
                skipped.append(safe_name); continue
            if candidate.suffix.lower() not in ALLOWED_PAYMENT_EXTS:
                skipped.append(safe_name); continue

            try:
                if candidate.exists() and candidate.is_file():
                    candidate.unlink()
                    deleted.append(safe_name)
                else:
                    skipped.append(safe_name)
            except Exception as e:
                skipped.append(f"{safe_name} (err: {e})")

        if deleted:
            flash(f"Supprimés : {', '.join(deleted)}")
        if skipped:
            flash(f"Non supprimés : {', '.join(skipped)}")

        return redirect(url_for('paiements_fichiers'))

    files = _list_payment_files()
    return render_template('paiements_fichiers.html', files=files)


def nettoyer_sauvegardes():
    dossier = Path(app.config['SAVEPAYMENT_FOLDER']).resolve()
    if dossier.exists():
        for f in dossier.iterdir():
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
    """Vérifie et sécurise le chemin du fichier demandé."""
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
    """Liste brute des fichiers dans la sauvegarde."""
    try:
        dossier = _dossier_sauvegarde()
        fichiers = []
        for f in sorted(dossier.iterdir()):
            if f.is_file():
                stat = f.stat()
                fichiers.append({
                    "nom": f.name,
                    "taille": stat.st_size,  # en octets
                    "modifie_le": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
                })

        return render_template('sauvegardes.html', fichiers=fichiers)

    except Exception as e:
        flash(f"Erreur lors de la lecture des sauvegardes : {e}", category='danger')
        return render_template('sauvegardes.html', fichiers=[])


@app.route('/sauvegardes/telecharger/<path:nom_fichier>', methods=['GET'])
def telecharger_sauvegarde(nom_fichier: str):
    """Télécharge un fichier depuis la sauvegarde."""
    try:
        chemin = _verifier_chemin_secure(nom_fichier)
        dossier = chemin.parent
        return send_from_directory(
            directory=str(dossier),
            path=chemin.name,
            as_attachment=True
        )
    except Exception as e:
        flash(f"Téléchargement impossible : {e}", category='danger')
        return redirect(url_for('lister_sauvegardes'))


@app.route('/sauvegardes/supprimer', methods=['POST'])
def supprimer_sauvegarde():
    """Supprime un fichier sélectionné."""
    nom_fichier = request.form.get('nom_fichier', '')
    try:
        chemin = _verifier_chemin_secure(nom_fichier)
        if not chemin.exists() or not chemin.is_file():
            flash("Fichier introuvable.", category='warning')
            return redirect(url_for('lister_sauvegardes'))

        os.remove(chemin)
        flash(f"Fichier « {chemin.name} » supprimé avec succès.", category='success')
    except Exception as e:
        flash(f"Suppression impossible : {e}", category='danger')

    return redirect(url_for('lister_sauvegardes'))

if __name__ == '__main__':
     app.run(debug=True,use_reloader=False, host='0.0.0.0', port=5003)
