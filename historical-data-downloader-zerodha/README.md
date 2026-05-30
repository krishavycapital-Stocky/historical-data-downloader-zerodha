# Historical Data Downloader — Zerodha

A free, self-hosted web app to download historical OHLCV + OI data from Zerodha Kite Connect API.

## Features
- NIFTY, BANKNIFTY, SENSEX — Options & Futures
- Equity (any NSE/BSE stock)
- GOLDBEES, NIFTYBEES (ETFs)
- Intervals: 1min to Day
- File formats: CSV, Parquet, JSON
- Bulk download via CSV upload
- Preview first 5 rows before download
- Pause / Resume bulk downloads
- Credentials saved in browser (no re-entry)
- Built-in daily token generator

---

## Deployment on Render.com (Free)

### Step 1 — Push code to GitHub
You already have this repo on GitHub. Done ✅

### Step 2 — Create Render account
1. Go to https://render.com
2. Click **Sign Up** → choose **Sign up with GitHub**
3. Authorize Render to access your GitHub

### Step 3 — Create Web Service
1. Click **New +** → **Web Service**
2. Connect your GitHub repo: `historical-data-downloader-zerodha`
3. Fill in:
   - **Name**: historical-data-downloader-zerodha
   - **Region**: Singapore (closest to India)
   - **Branch**: main
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `uvicorn main:app --host 0.0.0.0 --port $PORT`
4. Choose **Free** plan
5. Click **Create Web Service**

### Step 4 — Wait for deploy
Render will build and deploy in ~3-5 minutes.
Your live URL will be: `https://historical-data-downloader-zerodha.onrender.com`

---

## Daily Usage

1. Open your live URL
2. Go to **Login & Token** tab
3. Click **Open Kite Login Page** → login with Zerodha
4. Copy `request_token` from the redirect URL
5. Paste it and click **Generate Access Token**
6. Switch to **Download Data** tab → select instrument → download!

---

## Updating the App

When Claude gives you updated code:
1. Replace the files in your local folder
2. Push to GitHub (`git add . && git commit -m "update" && git push`)
3. Render auto-deploys within 2 minutes ✅

---

## Not affiliated with Zerodha. Use your own Kite Connect API credentials.
