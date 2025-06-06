import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# --- Carregar variáveis do .env ---
load_dotenv()
ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")
AD_ACCOUNT_ID = os.getenv("META_AD_ACCOUNT_ID")
API_VERSION = 'v23.0'
SHEET_NAME = 'InsightsMeta'
CREDENTIALS_FILE = './Credentials/arquivo-credenciais.json'
YEAR = 2025


# --- Campos de interesse nos insights ---
INSIGHT_FIELDS = (
    "campaign_id,campaign_name,reach,impressions,frequency,"
    "results,cost_per_result,spend,cpm,actions,cpc,date_start"
)

# --- Gerar todos os períodos do ano por mês ---
def get_month_ranges(year):
    month_ranges = []
    for month in range(1, 13):
        first = datetime(year, month, 1)
        if month == 12:
            last = datetime(year+1, 1, 1) - timedelta(days=1)
        else:
            last = datetime(year, month+1, 1) - timedelta(days=1)
        month_ranges.append({
            "since": first.strftime("%Y-%m-%d"),
            "until": last.strftime("%Y-%m-%d")
        })
    return month_ranges

# --- Buscar campanhas ---
def fetch_campaigns(start_date, end_date):
    url = f"https://graph.facebook.com/{API_VERSION}/{AD_ACCOUNT_ID}/campaigns"
    params = {
        "fields": "id,name",
        "access_token": ACCESS_TOKEN,
        "time_range[since]": start_date,
        "time_range[until]": end_date,
        "limit": 100
    }
    resposta = requests.get(url, params=params, timeout=30)
    resposta.raise_for_status()
    return resposta.json().get('data', [])

# --- Buscar insights de cada campanha no período, dia a dia ---
def fetch_campaign_insights(campaign_id, start_date, end_date):
    url = f"https://graph.facebook.com/{API_VERSION}/{campaign_id}/insights"
    params = {
        "fields": INSIGHT_FIELDS,
        "access_token": ACCESS_TOKEN,
        "time_range[since]": start_date,
        "time_range[until]": end_date,
        "time_increment": 1,
        "limit": 100
    }
    resposta = requests.get(url, params=params, timeout=30)
    resposta.raise_for_status()
    return resposta.json().get('data', [])

# --- Consolidar dados ---
def get_all_data(year):
    all_rows = []
    for period in get_month_ranges(year):
        print(f"Buscando de {period['since']} até {period['until']}...")
        try:
            campaigns = fetch_campaigns(period['since'], period['until'])
            for campaign in campaigns:
                insights = fetch_campaign_insights(campaign['id'], period['since'], period['until'])
                for insight in insights:
                    # Inclui nome/campanha em caso falte no insight
                    if "campaign_name" not in insight:
                        insight["campaign_name"] = campaign["name"]
                    all_rows.append(insight)
        except Exception as e:
            print(f"Erro no período {period['since']} até {period['until']}: {e}")
    return all_rows

# --- Upload para Google Sheets ---
def upload_to_google_sheets(df, sheet_name, credentials_file):
    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    credentials = ServiceAccountCredentials.from_json_keyfile_name(credentials_file, scopes)
    gc = gspread.authorize(credentials)
    try:
        sheet = gc.open(sheet_name).sheet1
    except gspread.SpreadsheetNotFound:
        sh = gc.create(sheet_name)
        sh.share("<seuemail>@gmail.com", perm_type="user", role="writer")
        sheet = sh.sheet1

    sheet.clear()
    sheet.update([df.columns.values.tolist()] + df.astype(str).values.tolist())

# --- Execução principal ---
if __name__ == "__main__":

    print(f"Iniciando extração do ano {YEAR}...")

    data = get_all_data(YEAR)
    if data:
        df = pd.DataFrame(data)
        print(f"Dados coletados: {len(df)} registros.")
        try:
            print("Enviando para o Google Sheets...")
            upload_to_google_sheets(df, SHEET_NAME, CREDENTIALS_FILE)
            print("Concluído com sucesso! 🚀")
        except Exception as e:
            print(f"Erro ao enviar para o Sheets: {e}")
    else:
        print("Nenhum dado encontrado.")