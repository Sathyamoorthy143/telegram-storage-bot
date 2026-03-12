FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["python", "bot.py"]
```

---

## Step 2 — Update `requirements.txt`

Make sure it has exactly this:
```
python-telegram-bot[job-queue]==21.*
cryptography
python-dotenv