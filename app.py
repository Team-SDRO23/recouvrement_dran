
from flask import Flask, render_template, request, redirect, url_for, session, flash,send_file
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
import os, io,csv, re
from io import BytesIO,StringIO
import numpy as np
import pandas as pd
from flask_sqlalchemy import SQLAlchemy
from pathlib import Path
from datetime import datetime
from utils import _read_any_file,_to_number_cfa,_load_all_payments,_load_impayes,normalize_phone_ci
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import getSampleStyleSheet



app = Flask(__name__)
app.secret_key = 'secret_key'
app.config['UPLOAD_FOLDER'] = 'impayefacture'
app.config['PAYMENT_FOLDER'] = 'payementfacture'

os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['PAYMENT_FOLDER'], exist_ok=True)


app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///users.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# ----------- Modèle User -----------
class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(50), unique=True, nullable=False)
    password = db.Column(db.String(200), nullable=False)


with app.app_context():
    db.create_all()

# ----------- Page d'accueil -----------
@app.route('/')
def home():
    if 'user_id' in session:
        return redirect(url_for('upload_liste_impaye'))
    return redirect(url_for('login'))
# ----------- Enregistrement -----------
@app.route('/register', methods=['GET','POST'])
def register():
    if request.method == 'POST':
        username = request.form['username'].strip()
        password_raw = request.form['password']

        if not username.endswith("@cie.ci"):
            flash("Le nom d'utilisateur doit être une adresse se terminant par @cie.ci")
            return render_template('register.html')
     
        if User.query.filter_by(username=username).first():
            flash('Utilisateur déjà existant')
            return render_template('register.html')

        password = generate_password_hash(password_raw)
        user = User(username=username, password=password)
        db.session.add(user)
        db.session.commit()
        flash('Inscription réussie')
        return redirect(url_for('login'))

    return render_template('register.html')

# ----------- Connexion -----------
@app.route('/login', methods=['GET','POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        user = User.query.filter_by(username=username).first()
        if user and check_password_hash(user.password, password):
            session['user_id'] = user.id
            return redirect(url_for('upload_liste_impaye'))
        else:
            flash('Identifiants invalides')
    return render_template('login.html')

# ----------- Déconnexion -----------
@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

# ----------- Tableau de bord -----------
@app.route('/dashboard')
def dashboard():
    if 'user_id' not in session:
        return redirect(url_for('login'))
    users = User.query.all()
    return render_template('dashboard.html', users=users)

@app.route('/delete_user/<int:id>')
def delete_user(id):
    if 'user_id' not in session:
        return redirect(url_for('login'))
    user = User.query.get(id)
    db.session.delete(user)
    db.session.commit()
    return redirect(url_for('dashboard'))
from io import BytesIO
import os
import numpy as np
import pandas as pd
from flask import request, session, redirect, url_for, render_template, flash
from werkzeug.utils import secure_filename

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
    if 'user_id' not in session:
        return redirect(url_for('login'))

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

            # 0) Lire le flux UNE SEULE FOIS
            raw = file.read()
            bio = BytesIO(raw)

            # 1) Lecture SANS entête pour pouvoir détecter la vraie ligne d’en-têtes
            if ext == 'csv':
                df0 = pd.read_csv(bio, header=None, dtype=object)
            else:
                df0 = pd.read_excel(bio, sheet_name=0, header=None, dtype=object)

            # 2) Détection robuste de la ligne d’en-têtes
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

            # 4c) drop colonnes & lignes totalement vides
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
    if 'user_id' not in session:
        return redirect(url_for('login'))

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
            required = {'Statut Branchement', 'Date statut', 'Genre client', 'Type branchement'}
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

                # Pour réécriture CSV plus tard
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
    import os
    import pandas as pd

    payment_folder = app.config['PAYMENT_FOLDER']
    if not os.path.isdir(payment_folder):
        raise FileNotFoundError("Le dossier de paiements n'existe pas.")

    pay = _load_all_payments(payment_folder)
    if pay.empty:
        raise ValueError("Aucun fichier de paiements (csv/xlsx) trouvé dans le dossier.")

    # --- Dates
    if 'DateCreation' in pay.columns:
        pay['DateCreation_dt'] = pd.to_datetime(pay['DateCreation'], errors='coerce', dayfirst=True)
    else:
        pay['DateCreation_dt'] = pd.NaT

    # Nettoyage minimal des références (utile pour les comparaisons)
    for col in ['RefContrat', 'RefFacture']:
        if col in pay.columns:
            pay[col] = pay[col].astype(str).str.strip()
        else:
            pay[col] = pd.NA

    # --- Trouver, par contrat, la facture la plus récente (selon DateCreation_dt)
    pay_clean = pay.dropna(subset=['DateCreation_dt']).copy()

    # Optionnel : départage si égalité de date -> on prend la plus grande RefFacture
    # (retirer le sort sur RefFacture si non souhaité)
    pay_clean = pay_clean.sort_values(['RefContrat', 'DateCreation_dt', 'RefFacture'])

    # idxmax sur la date -> récupère la ligne "la plus récente" par RefContrat
    idx = pay_clean.groupby('RefContrat', dropna=False)['DateCreation_dt'].idxmax()

    latest_facture_par_contrat = (
        pay_clean.loc[idx, ['RefContrat', 'RefFacture']]
                 .rename(columns={'RefFacture': 'LastRefFacture'})
                 .dropna(subset=['RefContrat'])
    )

    # --- Garder toutes les lignes de la facture la plus récente par contrat
    pay2 = (
        pay.merge(latest_facture_par_contrat, on='RefContrat', how='inner')
           .loc[lambda df: df['RefFacture'] == df['LastRefFacture']]
           .copy()
    )

    # --- Montants payés
    if 'MontantReglement' not in pay2.columns:
        pay2['MontantReglement'] = '0'

    pay2['MontantReglement_num'] = (
        pay2['MontantReglement'].apply(_to_number_cfa).fillna(0)
    )

    agg = (
        pay2.groupby(['RefContrat', 'RefFacture'], as_index=False, dropna=False)
            .agg(TotalPayes=('MontantReglement_num', 'sum'))
    )

    # --- Charger impayés
    imp_path = os.path.join(app.config['UPLOAD_FOLDER'], 'factureimpaye.xlsx')
    if not os.path.exists(imp_path):
        raise FileNotFoundError("Fichier des impayés introuvable (factureimpaye.xlsx).")

    imp = _load_impayes(imp_path)

    want_cols = ['RefContrat', 'Telephone_prive', 'Telephone_pro','Solde Total factures échues', 'ctr']
    for c in want_cols:
        if c not in imp.columns:
            imp[c] = pd.NA

    # Nettoyage des clés de jointure côté impayés
    imp['RefContrat'] = imp['RefContrat'].astype(str).str.strip()

    imp['Solde_num'] = imp['Solde Total factures échues'].apply(_to_number_cfa).fillna(0)
    imp['Telephone_prive'] = imp['Telephone_prive'].apply(normalize_phone_ci)
    imp['Telephone_pro'] = imp['Telephone_pro'].apply(normalize_phone_ci)
    imp['Telephone_prive'] = (
        imp['Telephone_prive'].fillna('') +
        imp['Telephone_pro'].fillna('').apply(lambda x: f"<br>{x}" if x else '')
    )

    # --- Fusion totaux payés vs soldes
    out_all = agg.merge(imp[want_cols + ['Solde_num']], on='RefContrat', how='left')

   
    out_all['Reste'] = (out_all['Solde_num'] - out_all['TotalPayes'])


    # --- Statistiques globales
    nb_clients_total = (
    pay['RefContrat']
    .astype(str)         
    .str.strip()         
    .replace('', pd.NA)   
    .dropna()
    .nunique()          
)

    nb_eligibles_total = (out_all['Reste'] <= 0).fillna(False).sum()

    stats = {
        'nb_clients_total':   int(nb_clients_total),
        'nb_eligibles_total': int(nb_eligibles_total),
        'total_payes_total':  float(out_all['TotalPayes'].sum(skipna=True)),
        'total_soldes_total': float(out_all['Solde_num'].sum(skipna=True)),
        'reste_total':        float(out_all['Reste'].sum(skipna=True)),
    }

    # --- Clients éligibles (reste <= 0)
    out = out_all[out_all['Reste'].fillna(1) <= 0].copy()

    # Affichage final (infos de contact + solde original)
    display = out[['RefContrat', 'ctr', 'Solde Total factures échues', 'Telephone_prive']].copy()
    display.sort_values(by=['RefContrat'], inplace=True, na_position='last')

    display.rename(columns={
        'RefContrat': 'numero_client',
        'ctr': 'num_compteur',
        'Solde Total factures échues': 'solde_total_facture',
        'Telephone_prive': 'telephone'
     
    }, inplace=True)

    display = display[['numero_client', 'num_compteur', 'solde_total_facture', 'telephone']]

    return display, stats



@app.route('/retablissements', methods=['GET'])
def retablissements():
    if 'user_id' not in session:
        return redirect(url_for('login'))

    try:
        display, stats = _compute_retablissemements(app)

       
        table_html = display.to_html(
            classes='table table-sm table-striped table-hover align-left',
            index=False, border=0, table_id='table-retab', escape=False
        )
        return render_template('retablissements.html', table_html=table_html, stats=stats)

    except Exception as e:
        flash(f"Erreur pendant le calcul : {e}")
        return render_template('retablissements.html', table_html=None, stats=None)


@app.route('/retablissements/download_excel', methods=['GET'])
def download_retab_excel():
    if 'user_id' not in session:
        return redirect(url_for('login'))
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
    if 'user_id' not in session:
        return redirect(url_for('login'))
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


if __name__ == '__main__':
    app.run(debug=True)
