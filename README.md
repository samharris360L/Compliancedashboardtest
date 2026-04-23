# 360Learning Live Dashboard

A public dashboard that summarises data from the 360Learning API v2, refreshed hourly by GitHub Actions and served via GitHub Pages.

## How it works

```
GitHub Actions (hourly)  ─►  fetch_data.py  ─►  data.json (committed)
                                                      │
                                                      ▼
                                         GitHub Pages serves index.html
                                         which fetches data.json client-side
```

The client-side page never sees your API credentials — only the pre-generated `data.json`. Your secret stays in GitHub Actions secrets.

## One-time setup

### 1. Create the repo

Push these files to a **public** GitHub repository (public is required for free GitHub Pages unless you have a paid plan).

### 2. Add your API credentials as repo secrets

In the repo: **Settings → Secrets and variables → Actions → New repository secret**

Add two secrets:

- `CLIENT_ID` — your 360Learning API v2 client ID
- `CLIENT_SECRET` — your 360Learning API v2 client secret

### 3. (Optional) Set the region

If you're on the US platform, go to **Settings → Secrets and variables → Actions → Variables tab** and add a repository variable:

- `BASE_URL` = `https://app.us.360learning.com`

Default is EU (`https://app.360learning.com`).

### 4. Grant the right scope to your API credential

In the 360Learning admin panel, make sure your API credential has the scope needed for the endpoint you're calling. The default script uses `/api/v2/courses`, which needs a courses read scope. If you see a 403 in the workflow logs, this is almost certainly why.

### 5. Enable GitHub Pages

**Settings → Pages → Build and deployment → Source: GitHub Actions**

(The workflow already deploys via the `actions/deploy-pages` action — you just need to select GitHub Actions as the source.)

### 6. Run the workflow once

**Actions tab → "Refresh data" → Run workflow**

After ~1 minute your site will be live at `https://<your-username>.github.io/<your-repo-name>/`.

It will refresh automatically every hour from then on.

## Files

| File | Purpose |
|---|---|
| `fetch_data.py` | Pulls the token, calls the API, writes `data.json` |
| `index.html` | The widget — loads `data.json` and renders it |
| `data.json` | Committed output; refreshed by the workflow |
| `.github/workflows/refresh.yml` | Hourly cron + Pages deploy |
| `requirements.txt` | Python deps (just `requests`) |

## Changing the data you display

1. Edit `fetch_data.py`:
   - Change the endpoint in `fetch_courses()` to whatever you need (users, groups, paths, etc.).
   - Change `summarise()` to shape the data the way the widget should read it.
2. Edit `index.html` to render the new shape.
3. Make sure your API credential has the scope for the new endpoint.

## Running locally for testing

```bash
pip install -r requirements.txt
export CLIENT_ID="your-id"
export CLIENT_SECRET="your-secret"
python fetch_data.py
# then serve the folder so fetch() can load data.json:
python -m http.server 8000
# visit http://localhost:8000
```

## Security

- Credentials live only in GitHub Actions secrets — never in the repo.
- The public site only ever sees the sanitised `data.json`. Make sure `summarise()` strips any fields you don't want publicly visible.
- The `data.json` file is committed to the repo, so anyone can see it. If you need the dashboard to be private, make the repo private and use GitHub Pages with restricted access (paid plan), or use a different approach.
