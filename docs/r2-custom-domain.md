# Serving R2 files from `files.over.org.il` instead of `r2.dev`

**Status:** planned, and **blocked on a prerequisite** — `over.org.il` is not yet
a zone in the Cloudflare account (see below). Once it is, the rest is one
dashboard action and one env change.
**Code change required: none.**

## Why

Every file OVER stores in R2 is served by redirecting to
`https://pub-63c02556dabd4956af9500eb8fe7198c.r2.dev/<key>`. That host is
Cloudflare's *development* URL for a bucket, and Cloudflare is explicit about it:

> Public access through `r2.dev` subdomains is rate-limited and should only be
> used for development purposes.
> — [Cloudflare R2 docs, Public buckets](https://developers.cloudflare.com/r2/buckets/public-buckets/)

It is not a soft limit. `r2.dev` sits behind Cloudflare's bot protection, which
blocks by client signature and returns **error 1010** — *"the owner of this
website has banned your access based on your browser's signature"* — as an HTML
body with a 403 status.

Measured against a real object on 2026-08-14:

| request `User-Agent` | result |
|---|---|
| `Python-urllib/3.14` (the default) | **403, Cloudflare error 1010** |
| `curl/8.4.0` | 206 ✓ |
| `python-requests/2.32.3` | 206 ✓ |
| `Mozilla/5.0 …` | 206 ✓ |

### What this actually cost

TAG-IT pulled the מבקר "דוחות שנתיים" bundle — **6.80 GB across 7 parts**
(1.00 / 0.99 / 0.97 / 0.99 / 0.97 / 1.00 / 0.88 GB) — and failed three times
with `[Errno 2] No such file or directory`: part 6 on the first run, part 7 on
the second, clean on the third. That is **~60 hours of downloading** to move one
dataset.

The error looked like a disk problem on their side and was not. A 403 arrives
where the ZIP should be; if the client does not check the status before writing,
no file is created, and the *next* step — the one that opens the file — reports
`[Errno 2]`. The failure surfaces one stage after its cause and names the wrong
subsystem.

It looked random because it is: `r2.dev` is rate-limited and bot-scored, so a
sustained multi-gigabyte pull from one IP raises its own score as it runs. The
first parts succeed; a later one does not; which one varies.

### We already knew, in one corner

`scripts/source_health_check.py:42` pins a browser User-Agent with the comment
*"Cloudflare (in front of r2.dev / odata) 403s the default Python-urllib agent,
which would falsely fail every download."* Someone hit this, understood it, and
worked around it **locally** — the fix never reached the path every external
consumer uses. Worth remembering: a workaround in a script is not a fix in a
system, and it hides the evidence that would have prompted one.

## Prerequisite: `over.org.il` must be a zone in the Cloudflare account

**This is the part that is not yet true, and it is the whole cost of the change.**

Cloudflare requires that *"the domain being used must have been added as a zone
in the same account as the R2 bucket"*. The domain does **not** need to use
Cloudflare nameservers — a partial (CNAME) setup satisfies it — but the zone
must exist in the account that owns the bucket.

Today it does not. Checked 2026-08-15:

```
over.org.il  nameserver = ns1.sitesdepot.com
over.org.il  nameserver = ns2.sitesdepot.com
```

Responses from `www.over.org.il` do carry `Server: cloudflare` and a `CF-RAY`
header, which looks like the domain is already on Cloudflare. It is not
evidence of that: Render fronts every `*.onrender.com` service with its OWN
Cloudflare, so those headers appear on any Render-hosted site regardless of who
holds the zone. Do not read them as "the zone is ours".

So step 1 is one of:

* **Full setup** — move `over.org.il` to Cloudflare nameservers at the registrar
  (internic). Cloudflare imports the existing records first; the cutover is the
  nameserver change. This also affects mail and every other record on the
  domain, so it is a real change to review, not a formality.
* **Partial (CNAME) setup** — add the zone in CNAME mode and keep DNS at
  sitesdepot. Historically a Business-plan feature; confirm it is available on
  the current plan before choosing this path.

## The change

1. Satisfy the prerequisite above — add `over.org.il` as a zone in the same
   Cloudflare account as the `over-files` bucket.

2. **Cloudflare dashboard** → R2 → the OVER bucket → *Settings* → *Public access*
   → **Custom domains** → *Connect domain* → `files.over.org.il`.
   Wait for the status to read *Active* (certificate issuance is usually a
   minute or two).

3. **Render** → `ckan-version-tracker` → *Environment* → set

   ```
   S3_PUBLIC_BASE_URL=https://files.over.org.il
   ```

   (It is currently the `pub-….r2.dev` host.)

4. Leave `r2.dev` public access **enabled** for now. Nothing OVER serves depends
   on it after step 2, but anyone holding an old URL keeps working. Disable it
   later, deliberately, not as part of this change.

## Why no code change

`StorageClient.public_url` builds every URL at request time from
`settings.s3_public_base_url`:

```python
base = settings.s3_public_base_url.rstrip("/")
return f"{base}/{key.lstrip('/')}"
```

Mappings persist the object *key* (`r2:datasets/<id>/v1/<hash>_part-1.zip`),
never a full URL. All three call sites — `app/api/v1.py:302`,
`app/api/versions.py:465`, `app/services/knesset_mmm_db.py:102` — resolve on the
way out. So the env change re-points every download link in the API, the
versions UI, the Looker connector and the MCP tools at once, with no migration
and no rewrite of stored rows.

## Verifying afterwards

```bash
curl -sS -o /dev/null -w '%{http_code} %{redirect_url}\n' "https://www.over.org.il/api/versions/eeaebc93-a866-4c0c-bc05-1b811d5f36b2/download/_zip_parts?index=0"
```

Expect a 307/302 to `https://files.over.org.il/...`. Then confirm the host no
longer discriminates by client — this is the whole point, so test the agent that
was blocked:

```bash
python -c "import urllib.request as u; print(u.urlopen(u.Request('<the redirect url>', headers={'Range':'bytes=0-0'})).status)"
```

Expect `206`. On `r2.dev` this exact call returns 403.

## Note for anyone downloading from OVER today

Until this lands, two things make a scripted download reliable:

* **Send an explicit `User-Agent`.** `curl/8.4.0` works; the Python default does not.
* **Check the status code before writing to disk.** A 403 written as a file, or
  not written at all, is what turns a network refusal into `[Errno 2]` three
  stages later — and into three 20-hour runs before anyone suspects the network.
