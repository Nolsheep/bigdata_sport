# NBA Games Data Pipeline (Kaggle → PostgreSQL → Google Sheets → Data Studio)

ไปป์ไลน์ข้อมูลอัตโนมัติแบบครบวงจรจากชุดข้อมูล Kaggle
[`nathanlauga/nba-games`](https://www.kaggle.com/datasets/nathanlauga/nba-games)
(ผลการแข่งขัน NBA ฤดูกาล 2003-04 ถึง 2022-23) ไปจนถึง Dashboard บน Looker Studio (Data Studio)
อัปเดตข้อมูลทั้งหมดได้ด้วยคำสั่งเดียว: `python run_pipeline.py`

## สถาปัตยกรรม

```
 Kaggle (kagglehub)          PostgreSQL 16 on Docker                Google Cloud
 ─────────────────          ───────────────────────────            ─────────────────────────────
 games.csv          ┐       ┌──────────────┐  transform.py ┌──────────────┐  publish.py  ┌───────────────┐   ┌─────────────┐
 games_details.csv  │ingest │  raw_data.*  │ ────pandas──▶ │ production.* │ ──gspread──▶ │ Google Sheets │──▶│ Data Studio │
 players.csv        ├──.py─▶│ (ข้อมูลดิบ)    │ clean/feature │ (พร้อมใช้งาน)   │              │ (1 ชีตต่อตาราง) │   │  Dashboard  │
 ranking.csv        │       └──────────────┘ agg/join      └──────────────┘              └───────────────┘   └─────────────┘
 teams.csv          ┘              ▲ ตรวจข้อมูลด้วย DBeaver ▲
                                   └──────── run_pipeline.py เรียกทุกขั้นตอนต่อกัน ────────┘
```

| ไฟล์ | หน้าที่ |
|---|---|
| `docker-compose.yml` | รัน PostgreSQL 16 (สร้าง schema `raw_data` และ `production` อัตโนมัติจาก `sql/init/`) |
| `ingest.py` | ดาวน์โหลด dataset ด้วย `kagglehub` แล้วโหลด CSV ทุกไฟล์เข้า `raw_data.<ชื่อไฟล์>` (ใช้ COPY ให้เร็ว) |
| `transform.py` | อ่าน `raw_data` → Cleaning, Feature Engineering, Aggregation, Joining ด้วย pandas → เขียนลง `production` |
| `publish.py` | อ่านตาราง `production` → เขียนลง Google Sheets (1 worksheet ต่อตาราง) ด้วย `gspread` + `gspread-dataframe` พร้อมตรวจงบจำนวน cell |
| `run_pipeline.py` | เรียก ingest → transform → publish ต่อเนื่องกัน |
| `db.py` | อ่านค่า `.env`, สร้าง SQLAlchemy engine, ฟังก์ชันเขียนตารางแบบ COPY |
| `sql/insights.sql` | SQL ตอบคำถามทั้ง 5 ข้อ สำหรับเปิดใน DBeaver |

### ตารางที่ได้

**raw_data** (ตรงตามไฟล์ Kaggle + คอลัมน์ `_source_file`, `_loaded_at`):
`games`, `games_details`, `players`, `ranking`, `teams`

**production**

| ตาราง | ระดับข้อมูล | สาระสำคัญ |
|---|---|---|
| `dim_teams` | 1 แถว/ทีม | ชื่อเต็มทีม, ตัวย่อ, สนาม |
| `fact_games` | 1 แถว/เกม | ลบเกมซ้ำ/ไม่มีคะแนน, join ชื่อทีม, `game_type` (ถอดจากหลักแรกของ GAME_ID), `season_label`, `total_points`, `margin`, `game_closeness`, `winner_team`, ส่วนต่างสถิติ |
| `team_games` | 1 แถว/ทีม/เกม | แปลง home/away เป็น long format |
| `team_season_stats` | 1 แถว/ทีม/ฤดูกาล | W-L, win %, PPG, แต้มที่เสีย, home/away win %, อันดับ + join conference จาก `ranking` |
| `player_season_stats` | 1 แถว/ผู้เล่น/ฤดูกาล | ค่าเฉลี่ยต่อเกม, FG%/3P%/FT%, True Shooting %, อันดับดาวซัลโว (≥40 เกม) |
| `season_summary` | 1 แถว/ฤดูกาล | แต้มเฉลี่ย, home win %, สัดส่วนเกมสูสี/ขาดลอย, 3PA ต่อเกม, 3PA rate |
| `win_factors` | 1 แถว/สถิติ/ฤดูกาล | ทีมที่ชนะสถิตินั้น ๆ ชนะเกมกี่ % |

## วิธีรันโปรเจค

### 1. เตรียมเครื่อง
- Docker Desktop, Python 3.10+, (ตัวเลือก) DBeaver

```bash
git clone https://github.com/Nolsheep/bigdata_sport.git
cd bigdata_sport
python -m venv .venv
# Windows: .venv\Scripts\activate   |   macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env        # Windows: copy .env.example .env
```

### 2. เปิด PostgreSQL
```bash
docker compose up -d
docker compose ps           # ต้องขึ้น healthy
```

### 3. ตั้งค่า Kaggle (ถ้าจำเป็น)
dataset นี้เป็น public ปกติ `kagglehub` ดาวน์โหลดได้เลย ถ้าเจอ error เรื่องสิทธิ์:
Kaggle → Settings → API → **Create New Token** แล้ววาง `kaggle.json` ไว้ที่ `~/.kaggle/kaggle.json`
(Windows: `C:\Users\<user>\.kaggle\kaggle.json`)

### 4. ตั้งค่า Google API Credentials (สำหรับ publish.py)
1. ไปที่ <https://console.cloud.google.com/> → สร้าง Project ใหม่ (เช่น `nba-pipeline`)
2. **APIs & Services → Library** → เปิด (Enable) **Google Sheets API** และ **Google Drive API**
3. **APIs & Services → Credentials → Create Credentials → Service account** → ตั้งชื่อ → Done
4. คลิก service account ที่สร้าง → แท็บ **Keys → Add key → Create new key → JSON** → ไฟล์จะถูกดาวน์โหลด
5. เปลี่ยนชื่อไฟล์เป็น `credentials.json` แล้ววางไว้ที่ root ของโปรเจค
   (**ห้าม commit** — ไฟล์นี้อยู่ใน `.gitignore` แล้ว)
6. สร้าง Google Sheets ว่าง 1 ไฟล์ → กด **Share** → ใส่อีเมลของ service account
   (`client_email` ใน credentials.json เช่น `xxx@nba-pipeline.iam.gserviceaccount.com`) สิทธิ์ **Editor**
7. คัดลอก Spreadsheet ID จาก URL `https://docs.google.com/spreadsheets/d/<ID>/edit` ใส่ใน `.env`:
   `GOOGLE_SHEET_ID=<ID>`

### 5. รันไปป์ไลน์
```bash
python run_pipeline.py                  # ingest -> transform -> publish
python run_pipeline.py --skip-publish   # ทำเฉพาะในเครื่อง (ยังไม่มี credentials)
python run_pipeline.py --steps publish  # ส่งขึ้น Google Sheets อย่างเดียว
```
หรือรันทีละขั้น: `python ingest.py`, `python transform.py`, `python publish.py`

### 6. ตรวจข้อมูลด้วย DBeaver
New Database Connection → **PostgreSQL**
Host `localhost` · Port `5432` · Database `nba_dw` · User `nba` · Password `nba_password`
แล้วเปิด `sql/insights.sql` เพื่อรันคำถามทั้ง 5 ข้อ

## ขนาดข้อมูลกับ Google Sheets
Google Sheets รับได้สูงสุด 10 ล้าน cell ต่อไฟล์ `publish.py` จึงส่งเฉพาะตารางสรุป
(`season_summary`, `team_season_stats`, `player_season_stats` ที่เล่น ≥ 10 เกม, `win_factors`,
`fact_games` เฉพาะ Regular Season/Playoffs/Play-In) และนับ cell ก่อนส่ง ถ้าเกิน 9 ล้านจะหยุดพร้อมแจ้งเตือน
ตารางระดับผู้เล่นต่อเกม (`games_details` ~670k แถว) เก็บไว้ใน PostgreSQL เท่านั้น

## Dashboard บน Data Studio (Looker Studio)
1. <https://lookerstudio.google.com> → **Create → Data source → Google Sheets** → เลือกไฟล์และ worksheet
   (เพิ่มทีละชีต: `season_summary`, `team_season_stats`, `player_season_stats`, `win_factors`, `fact_games`)
2. **Create → Report** แล้วเพิ่ม 5 visualization:

| # | คำถาม | ชีต | กราฟ |
|---|---|---|---|
| 1 | ความได้เปรียบในบ้าน (home-court advantage) ยังมีอยู่ไหม และเปลี่ยนไปอย่างไร | `season_summary` | Line: `season_label` × `home_win_pct` |
| 2 | เกมบุก/การยิง 3 แต้มเปลี่ยนไปอย่างไรในแต่ละฤดูกาล | `season_summary` | Combo: bar `avg_team_3pa` + line `avg_team_points` |
| 3 | ทีมไหนประสบความสำเร็จที่สุด | `team_season_stats` | Bar: `team_name` × SUM(`wins`) / win % + filter `season_label` |
| 4 | ใครคือดาวซัลโวของแต่ละฤดูกาล | `player_season_stats` | Table/Bar: Top 10 `player_name` by `ppg` (filter `games` ≥ 40) |
| 5 | สถิติตัวไหนสำคัญต่อชัยชนะที่สุด | `win_factors` | Bar: `stat_label` × `win_pct_when_better` (filter `season_label = All seasons`) |

เพิ่มเติม: Scorecard จำนวนเกม/แต้มเฉลี่ย, Pie `game_closeness` จาก `fact_games`, ตัวกรอง `season_label`
3. **Share → Manage access → Anyone with the link → Viewer**

## หมายเหตุข้อมูล
- `SEASON` ใน Kaggle คือปีที่ฤดูกาลเริ่ม (2022 = 2022-23); ฤดูกาล 2022-23 ในชุดข้อมูลมีเพียงบางส่วน
- หลักแรกของ `GAME_ID`: 1 = Preseason, 2 = Regular Season, 4 = Playoffs, 5 = Play-In
- ฤดูกาล 2019-20 และ 2020-21 ได้รับผลกระทบจาก COVID-19 (เกมน้อยลง และ 2020 เล่นแบบไม่มีผู้ชม)
