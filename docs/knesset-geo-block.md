# חסימה גיאוגרפית של אתר הכנסת, ויציאה דרך ישראל

## מה קורה

`knesset.gov.il` חוסם כתובות מחוץ לישראל. כל בקשה מהשרת של גרסאות לעם (xhostd, בחו"ל)
נענית ב-`303 See Other` אל `https://www.knesset.gov.il/maintenance-page-geo`, שעונה 200.
אומת בלוגים של הייצור ב-2026-10-31: מראת ה-ODATA (סכמת `knesset`) לא התעדכנה מאז 2026-10-28 לפחות.

מה נפגע כשאין יציאה דרך ישראל:

| מה | קוד | התנהגות בזמן חסימה |
|---|---|---|
| מראת ODATA (טיק כל 3 דקות) | `app/services/knesset_db.py` `sync_tick` | WARNING אחד בשורה אחת, ואז הפסקה של שעה (`GEO_BLOCK_BACKOFF`). בלי traceback. |
| `GET /api/knesset-db/tables`, `/schema.txt` | `app/api/knesset_db.py` | הרשימה נבנית מ-`knesset.sync_state` (הנתונים שכבר במראה נשארים זמינים). רק אם המראה מעולם לא נבנתה: 503 עם הודעה בעברית. |
| אצוות פרוטוקולים (ZIP) | `app/api/knesset_db.py` | קובץ שהופנה לדף החסימה נרשם ב-`_errors.txt` ולא נארז. |
| אימות כתובת ועדה ו-bootstrap של כנסת 25 | `app/api/knesset.py` | נכשלים (שם הוועדה חוזר ריק). |
| בדיקת המערכת `upstream.sources` | `app/services/health_checks.py` | "כנסת — ODATA" באזהרה: "הופנה ל-...maintenance-page-geo". |

מה **לא** נפגע: גירוד הפרוטוקולים של הוועדות (`knesset-committee-single-*`) ושל מרכז המחקר והמידע.
אותם מבצע ה-worker החיצוני (`_create_scrape_task`), ולא השרת עצמו.

## התיקון: `KNESSET_PROXY_URL`

כל בקשה של השרת לאתר הכנסת עוברת דרך `app/services/knesset_http.py`.
כשמוגדר `KNESSET_PROXY_URL`, בקשות ל-`knesset.gov.il` ולכל תת-דומיין שלו (`www.`, `main.`, `fs.`) יוצאות דרך ה-proxy.
**רק** הן: R2, ‏data.gov.il וכל השאר ממשיכים לצאת ישירות, גם כשהם עוברים באותו client.

```
KNESSET_PROXY_URL=http://over:<secret>@<il-host>:8888
```

את הערך מגדירים כמשתנה סביבה ב-xhostd (`set_secret`). הוא לא נכנס ל-repo.
אחרי שמגדירים אותו צריך restart. ה-restart גם מנקה את ההפסקה של השעה.

### הרצת ה-proxy על מכונת worker בישראל

מספיק forward proxy שמכיר `CONNECT` (כל התעבורה לכנסת היא HTTPS, אז ה-proxy רואה רק את שם השרת ולא את התוכן).
שני כללים חשובים: **רק דומיינים של הכנסת**, ו-**רק עם סיסמה**. אחרת זה proxy פתוח בבית שלכם.

**אפשרות א: tinyproxy** (‏Debian/Ubuntu: `apt install tinyproxy`, גרסה 1.11 ומעלה)

`/etc/tinyproxy/tinyproxy.conf`:

```
Port 8888
Listen 0.0.0.0
Timeout 120
MaxClients 20
BasicAuth over <secret>
ConnectPort 443
Filter "/etc/tinyproxy/knesset.filter"
FilterType ere
FilterDefaultDeny Yes
# Allow שורה לכל כתובת שממנה מותר להתחבר. אם כתובת היציאה של xhostd ידועה, הגבילו אליה.
Allow 0.0.0.0/0
```

`/etc/tinyproxy/knesset.filter`:

```
^(.+\.)?knesset\.gov\.il$
```

ואז `systemctl restart tinyproxy`, ובראוטר פותחים port forward ל-8888 (או port אחר שתבחרו).

**אפשרות ב: proxy קטן בפייתון** (בלי התקנות, `python3 knesset_proxy.py`, עם systemd או `nohup`)

```python
# knesset_proxy.py: CONNECT-only, knesset.gov.il בלבד, עם Basic auth. PROXY_SECRET="user:pass".
import asyncio, base64, os, re
AUTH = "Basic " + base64.b64encode(os.environ["PROXY_SECRET"].encode()).decode()
ALLOW = re.compile(r"^(.+\.)?knesset\.gov\.il:443$", re.I)

async def pipe(r, w):
    try:
        while data := await r.read(65536):
            w.write(data); await w.drain()
    finally:
        w.close()

async def handle(cr, cw):
    head = (await cr.readuntil(b"\r\n\r\n")).decode("latin-1").split("\r\n")
    method, target, _ = head[0].split(" ", 2)
    hdrs = {k.lower(): v.strip() for k, _, v in (h.partition(":") for h in head[1:] if h)}
    if method != "CONNECT" or not ALLOW.match(target):
        cw.write(b"HTTP/1.1 403 Forbidden\r\n\r\n"); return cw.close()
    if hdrs.get("proxy-authorization") != AUTH:
        cw.write(b'HTTP/1.1 407 Proxy Auth Required\r\nProxy-Authenticate: Basic realm="k"\r\n\r\n'); return cw.close()
    host, port = target.rsplit(":", 1)
    try:
        ur, uw = await asyncio.open_connection(host, int(port))
    except OSError:
        cw.write(b"HTTP/1.1 502 Bad Gateway\r\n\r\n"); return cw.close()
    cw.write(b"HTTP/1.1 200 Connection established\r\n\r\n"); await cw.drain()
    await asyncio.gather(pipe(cr, uw), pipe(ur, cw), return_exceptions=True)

async def main():
    srv = await asyncio.start_server(handle, "0.0.0.0", int(os.environ.get("PORT", "8888")))
    async with srv: await srv.serve_forever()

asyncio.run(main())
```

### בדיקה

מכל מכונה (גם מחוץ לישראל):

```bash
curl -sS -x "http://over:<secret>@<il-host>:8888" -o /dev/null -w '%{http_code} %{redirect_url}\n' \
  'https://knesset.gov.il/OdataV4/ParliamentInfo/KNS_KnessetDates?$top=1'
# 200 וכתובת ריקה: עובד. 303 עם maintenance-page-geo: ה-proxy לא יוצא מישראל.
curl -sS -x "http://over:<secret>@<il-host>:8888" https://example.com   # חייב להיכשל (403)
```

אחרי ההגדרה בייצור: הריצו את הבדיקה `upstream.sources` בטאב בדיקות מערכת. היא עוברת דרך אותו proxy.
אחר כך `GET /api/knesset-db/status` אמור להראות `last_synced_at` מתקדם תוך כמה טיקים.

### הערות אבטחה

- הסיסמה עוברת ל-proxy ב-HTTP רגיל (ה-`Proxy-Authorization` של ה-CONNECT). התוכן עצמו מוצפן מקצה לקצה מול הכנסת.
  ה-proxy מוגבל לדומיינים של הכנסת, ולכן סיסמה שדלפה שווה מעט. מי שרוצה יותר: מנהרת WireGuard/SSH במקום port פתוח.
- הגבילו `Allow` (או את חומת האש) לכתובת היציאה של xhostd כשהיא ידועה.
- אל תכניסו את הסיסמה ל-`render.yaml`, ל-`xhostd-env.sh` או לכל קובץ ב-repo.
