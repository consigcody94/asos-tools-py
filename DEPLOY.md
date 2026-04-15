# Deploying the ASOS Tools dashboard

The [`app.py`](app.py) Streamlit app is self-contained and works on any host
that can run Python 3.9+ with the libraries listed in [`requirements.txt`](requirements.txt).

## TL;DR — pick one

| If you want…                                                | use                              |
| ----------------------------------------------------------- | -------------------------------- |
| Fastest path, zero config, no cold starts                   | **Hugging Face Spaces**          |
| Streamlit-native URL (`*.streamlit.app`)                    | **Streamlit Community Cloud**    |
| Custom domain + always-on, production-grade                 | **Fly.io**                       |
| GitHub-auto-deploy with paid managed tier                   | Railway                          |
| Serverless with cold starts (works but slower)              | Render                           |

All the code you need is already here; the only difference is which platform
you copy it to.

---

## 1. Hugging Face Spaces (recommended)

The **fastest path** — about 3 minutes.

1. Go to <https://huggingface.co/new-space>
2. **Name** it (e.g. `asos-tools`), **SDK** = *Streamlit*, public.
3. Once the Space repo exists, clone it:
   ```bash
   git clone https://huggingface.co/spaces/<your-user>/asos-tools
   cd asos-tools
   ```
4. Copy the following from this repo into it:
   ```
   app.py
   requirements.txt
   asos_tools/
   ```
5. Copy the Spaces README (with YAML frontmatter) into the Space's `README.md`:
   ```bash
   cp deploy/huggingface_README.md README.md
   ```
6. Push:
   ```bash
   git add .
   git commit -m "Initial deploy"
   git push
   ```

HF Spaces will install the requirements and boot the app automatically. Free
tier gets you 2 vCPU and 16 GB RAM with no cold starts.

---

## 2. Streamlit Community Cloud

1. Sign in at <https://streamlit.io/cloud> with your GitHub account.
2. Click **New app**, pick this repo, branch `main`, file `app.py`.
3. Done — your app is at `https://<repo-name>-<hash>.streamlit.app`.

Pros: zero config, native Streamlit URL.
Caveat: free-tier apps sleep after ~7 days of idle and take ~30 seconds to wake.

---

## 3. Fly.io (production-grade)

Install [`flyctl`](https://fly.io/docs/flyctl/install/), then:

```bash
fly launch --no-deploy                                # generates fly.toml
# Choose a name, region (iad/den/lhr/...), say NO to Postgres/Redis.
fly deploy
```

Create a `Dockerfile` (if `fly launch` didn't make one):

```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY asos_tools/ ./asos_tools/
COPY app.py .
EXPOSE 8501
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
```

Free tier = 3 × shared-cpu-1x 256 MB VMs. The dashboard fits comfortably.

---

## 4. Railway

1. Sign in at <https://railway.app> with your GitHub account.
2. **New Project → Deploy from GitHub repo**.
3. Railway auto-detects `requirements.txt` and picks up the start command.
   If it doesn't, set **Start Command** to:
   ```
   streamlit run app.py --server.port=$PORT --server.address=0.0.0.0
   ```
4. Generate a public domain under **Settings → Networking**.

Free trial ~$5 of credit; paid tier ~$5–10/month for an always-on dashboard.

---

## 5. Render

1. Sign in at <https://render.com>, connect GitHub.
2. **New → Web Service**, pick this repo.
3. Environment: **Python**, Build Command: `pip install -r requirements.txt`,
   Start Command: `streamlit run app.py --server.port=$PORT --server.address=0.0.0.0`.

Free tier has ~30 s cold-start delay after 15 minutes of inactivity.

---

## Hosts that are NOT a good fit

- **Vercel, Netlify, Cloudflare Pages** — built for static / serverless edge.
  Python serverless functions exist but cold-start cost of importing pandas +
  matplotlib is painful, and Vercel's 10-second Python timeout is tight for a
  30-day metar fetch.
- **AWS Lambda / GCP Cloud Run (raw)** — possible but requires bundling a
  matplotlib-compatible layer and hand-wiring HTTP; not worth the effort for
  this size of app.

## Running locally

```bash
pip install -r requirements.txt
streamlit run app.py
```

Open <http://localhost:8501>.
