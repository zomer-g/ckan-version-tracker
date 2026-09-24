/**
 * שקיפות מחירים — every food retailer's published prices, as one market.
 *
 * The law makes each large chain publish its stores, every item's price at
 * every store and every promotion — each on its own site, in its own format.
 * OVER collects each chain as its own dataset (~30 of them); this page is where
 * they are read together.
 *
 * Four tabs:
 *   compare — find an item by name or barcode, see its price at every chain
 *   basket  — a list of items, the stores in a city ranked by what it costs
 *   table   — the uniform tables themselves: every chain in one schema, with
 *             filters, CSV, the API URL and the same query in /data
 *   chains  — who is collected, how many stores, how fresh
 *
 * Everything lives in the query string so every view is a shareable link, the
 * convention /data and עסקאות נדל"ן follow.
 */
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";
import {
  prices as pricesApi, PriceBasketStore, PriceChain, PriceCompareItem, PriceProduct,
  PricePromotion, PriceTableResult, PriceTableSpec,
} from "../api/client";
import { useDocumentTitle } from "../hooks/useDocumentTitle";

type Tab = "compare" | "basket" | "table" | "chains";
const TABS: [Tab, string][] = [
  ["compare", "🔎 השוואת מחיר"],
  ["basket", "🛒 סל קניות"],
  ["table", "🗂 הטבלה האחידה"],
  ["chains", "🏪 הרשתות"],
];

const NIS = new Intl.NumberFormat("he-IL", { style: "currency", currency: "ILS", minimumFractionDigits: 2 });

function Ltr({ children }: { children: React.ReactNode }) {
  return <span style={{ unicodeBidi: "isolate", direction: "ltr", display: "inline-block" }}>{children}</span>;
}

function heDate(iso: string | null | undefined): string {
  if (!iso) return "—";
  const [y, m, d] = iso.slice(0, 10).split("-");
  return d ? `${d}/${m}/${y}` : iso;
}

function money(v: number | null | undefined): string {
  return v == null ? "—" : NIS.format(v);
}

const cell: React.CSSProperties = { padding: "0.3rem 0.45rem", textAlign: "start", verticalAlign: "top" };
const btn: React.CSSProperties = {
  padding: "0.35rem 0.8rem", fontSize: "0.85rem", cursor: "pointer",
  border: "1px solid var(--border)", borderRadius: 6, background: "none",
};
const primaryBtn: React.CSSProperties = {
  ...btn, background: "var(--primary)", color: "#fff", borderColor: "var(--primary)",
};

function ErrorLine({ error }: { error: string | null }) {
  return error ? <div className="text-sm" role="alert" style={{ color: "var(--danger)", margin: "0.4rem 0" }}>{error}</div> : null;
}

/** Name-or-barcode search, shared by the compare and basket tabs. */
function ProductSearch({ initial, onQuery, actionLabel, onPick }: {
  initial: string; onQuery: (q: string) => void; actionLabel: string;
  onPick: (p: PriceProduct) => void;
}) {
  const [q, setQ] = useState(initial);
  const [items, setItems] = useState<PriceProduct[] | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Submitting also writes q to the URL, which re-feeds `initial`: one request.
  const lastRun = useRef("");
  const run = useCallback((text: string) => {
    if (text.trim().length < 2 || text.trim() === lastRun.current) return;
    lastRun.current = text.trim();
    setLoading(true); setError(null);
    pricesApi.products(text.trim(), 30)
      .then((r) => setItems(r.items))
      .catch((e) => { setError(String(e.message || e)); setItems(null); lastRun.current = ""; })
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => { if (initial) run(initial); }, [initial, run]);

  return (
    <div>
      <form className="flex" style={{ gap: "0.5rem", flexWrap: "wrap", alignItems: "flex-end" }}
            onSubmit={(e) => { e.preventDefault(); onQuery(q.trim()); run(q); }}>
        <label className="text-sm" style={{ flex: "1 1 260px" }}>
          שם מוצר או ברקוד
          <br />
          <input value={q} onChange={(e) => setQ(e.target.value)} placeholder="למשל: חלב 3% או 7290000066134"
                 style={{ padding: "0.4rem 0.55rem", width: "100%", maxWidth: 420 }} />
        </label>
        <button type="submit" style={primaryBtn}>חיפוש</button>
      </form>
      {loading && <div className="text-sm text-muted" style={{ marginTop: "0.5rem" }}>מחפש בכל הרשתות…</div>}
      <ErrorLine error={error} />
      {items && items.length === 0 && <div className="text-sm text-muted" style={{ marginTop: "0.5rem" }}>לא נמצא מוצר כזה באף רשת.</div>}
      {items && items.length > 0 && (
        <div tabIndex={0} role="region" aria-label="תוצאות חיפוש מוצר" className="scroll-region"
             style={{ overflowX: "auto", marginTop: "0.6rem", maxHeight: 340, overflowY: "auto" }}>
          <table style={{ width: "100%", fontSize: "0.86rem", borderCollapse: "collapse" }}>
            <thead>
              <tr style={{ color: "var(--text-muted)" }}>
                {["מוצר", "יצרן", "גודל", "ברקוד", "רשתות", ""].map((h) => <th key={h} scope="col" style={cell}>{h}</th>)}
              </tr>
            </thead>
            <tbody>
              {items.map((p) => (
                <tr key={p.item_code} style={{ borderTop: "1px solid var(--border)" }}>
                  <td style={cell}>{p.names[0] ?? "—"}
                    {p.names.length > 1 && <span className="text-muted" title={p.names.slice(1).join(" · ")}> (+{p.names.length - 1} שמות)</span>}
                  </td>
                  <td style={cell}>{p.manufacturer || "—"}</td>
                  <td style={cell}><Ltr>{[p.quantity, p.unit_qty].filter(Boolean).join(" ") || "—"}</Ltr></td>
                  <td style={cell}><Ltr>{p.item_code}</Ltr></td>
                  <td style={cell}><Ltr>{p.chains.length}</Ltr></td>
                  <td style={cell}><button type="button" style={btn} onClick={() => onPick(p)}>{actionLabel}</button></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

// ─────────────────────────────────────────────────────────────── compare ──

function CompareTab({ params, patch }: { params: URLSearchParams; patch: (p: Record<string, string | null>) => void }) {
  const item = params.get("item") || "";
  const city = params.get("city") || "";
  const [cityInput, setCityInput] = useState(city);
  const [data, setData] = useState<PriceCompareItem | null>(null);
  const [notFound, setNotFound] = useState(false);
  const [promos, setPromos] = useState<PricePromotion[] | null>(null);
  const [allPromos, setAllPromos] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!item) { setData(null); return; }
    setLoading(true); setError(null); setNotFound(false);
    pricesApi.compare([item], city || undefined, 10)
      .then((r) => { setData(r.items[0] ?? null); setNotFound(r.items.length === 0); })
      .catch((e) => setError(String(e.message || e)))
      .finally(() => setLoading(false));
    setPromos(null);
    pricesApi.promotions(item).then((r) => setPromos(r.promotions)).catch(() => setPromos([]));
  }, [item, city]);

  return (
    <>
      <ProductSearch initial={params.get("q") || ""} actionLabel="השוואה"
                     onQuery={(q) => patch({ q })}
                     onPick={(p) => patch({ item: p.item_code })} />

      {item && (
        <section style={{ marginTop: "1.2rem" }} aria-live="polite">
          <form className="flex" style={{ gap: "0.5rem", alignItems: "flex-end", flexWrap: "wrap", marginBottom: "0.6rem" }}
                onSubmit={(e) => { e.preventDefault(); patch({ city: cityInput.trim() || null }); }}>
            <h2 style={{ margin: 0, fontSize: "1.15rem", flex: "1 1 auto" }}>
              {data?.item_name ?? "מוצר"} <span className="text-muted text-sm"><Ltr>{item}</Ltr></span>
            </h2>
            <label className="text-sm">
              עיר (לא חובה)
              <br />
              <input value={cityInput} onChange={(e) => setCityInput(e.target.value)} placeholder="כל הארץ"
                     style={{ padding: "0.35rem 0.5rem", width: 150 }} />
            </label>
            <button type="submit" style={btn}>סינון</button>
          </form>

          {loading && <div className="text-sm text-muted">משווה בין הרשתות…</div>}
          <ErrorLine error={error} />
          {notFound && !loading && <div className="text-sm text-muted">המוצר לא נמכר כרגע באף סניף{city ? ` ב${city}` : ""}.</div>}

          {data && (
            <>
              <div className="text-sm text-muted" style={{ marginBottom: "0.4rem" }}>
                <Ltr>{data.store_count.toLocaleString("he-IL")}</Ltr> סניפים ב-<Ltr>{data.chains.length}</Ltr> רשתות
                {city ? ` ב${city}` : ""}. המחיר הוא המחיר בקובץ האחרון שכל סניף פרסם; מבצעים מוצגים בנפרד.
              </div>
              <div tabIndex={0} role="region" aria-label="מחיר לפי רשת" className="scroll-region" style={{ overflowX: "auto" }}>
                <table style={{ width: "100%", fontSize: "0.86rem", borderCollapse: "collapse" }}>
                  <thead>
                    <tr style={{ color: "var(--text-muted)" }}>
                      {["רשת", "סניפים", "הזול", "חציון", "היקר", "הסניף הזול", "נכון ל"].map((h) => <th key={h} scope="col" style={cell}>{h}</th>)}
                    </tr>
                  </thead>
                  <tbody>
                    {data.chains.map((c, i) => (
                      <tr key={c.chain} style={{ borderTop: "1px solid var(--border)", fontWeight: i === 0 ? 700 : 400 }}>
                        <td style={cell}>{c.chain_name}</td>
                        <td style={cell}><Ltr>{c.stores}</Ltr></td>
                        <td style={cell}><Ltr>{money(c.min_price)}</Ltr></td>
                        <td style={cell}><Ltr>{money(c.median_price)}</Ltr></td>
                        <td style={cell}><Ltr>{money(c.max_price)}</Ltr></td>
                        <td style={cell}>{[c.cheapest_store.store_name, c.cheapest_store.city].filter(Boolean).join(", ") || "—"}</td>
                        <td style={cell}><Ltr>{heDate(c.as_of)}</Ltr></td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              <h3 style={{ fontSize: "1rem", margin: "1.1rem 0 0.4rem" }}>הסניפים הזולים ביותר</h3>
              <ol className="text-sm" style={{ margin: 0, paddingInlineStart: "1.3rem", lineHeight: 1.8 }}>
                {data.cheapest_stores.map((s) => (
                  <li key={`${s.chain}-${s.sub_chain_id}-${s.store_id}`}>
                    <strong><Ltr>{money(s.price)}</Ltr></strong> · {s.chain_name} · {s.store_name ?? `סניף ${s.store_id}`}
                    {s.city ? `, ${s.city}` : ""}{s.address ? ` (${s.address})` : ""}
                    <span className="text-muted"> · מאז <Ltr>{heDate(s.since)}</Ltr></span>
                  </li>
                ))}
              </ol>
            </>
          )}

          {promos && promos.length > 0 && (
            <>
              <h3 style={{ fontSize: "1rem", margin: "1.1rem 0 0.4rem" }}>מבצעים פעילים על המוצר</h3>
              <div tabIndex={0} role="region" aria-label="מבצעים" className="scroll-region" style={{ overflowX: "auto" }}>
                <table style={{ width: "100%", fontSize: "0.84rem", borderCollapse: "collapse" }}>
                  <thead>
                    <tr style={{ color: "var(--text-muted)" }}>
                      {["רשת", "המבצע", "מחיר מבצע", "כמות מינ׳", "עד", "סניפים", "מועדון"].map((h) => <th key={h} scope="col" style={cell}>{h}</th>)}
                    </tr>
                  </thead>
                  <tbody>
                    {(allPromos ? promos : promos.slice(0, 10)).map((p) => (
                      <tr key={`${p.chain}-${p.promotion_id}`} style={{ borderTop: "1px solid var(--border)" }}>
                        <td style={cell}>{p.chain_name}</td>
                        <td style={cell}>{p.description || "—"}</td>
                        <td style={cell}><Ltr>{p.discounted_price || p.promo_discounted_price || "—"}</Ltr></td>
                        <td style={cell}><Ltr>{p.min_qty || p.promo_min_qty || "—"}</Ltr></td>
                        <td style={cell}><Ltr>{heDate(p.end_date)}</Ltr></td>
                        <td style={cell}><Ltr>{p.stores}</Ltr></td>
                        <td style={cell}>{p.club_id && p.club_id !== "0" ? "כן" : "—"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              {promos.length > 10 && (
                <button type="button" style={{ ...btn, marginTop: "0.4rem" }} onClick={() => setAllPromos(!allPromos)}>
                  {allPromos ? "פחות" : `כל ${promos.length} המבצעים`}
                </button>
              )}
            </>
          )}
          <div className="text-sm" style={{ marginTop: "0.8rem" }}>
            <a href={`/projects/prices?tab=table&t=prices_market&item_code=${encodeURIComponent(item)}${city ? `&city=${encodeURIComponent(city)}` : ""}`}>
              כל השורות של המוצר בטבלה האחידה ←
            </a>
          </div>
        </section>
      )}
    </>
  );
}

// ──────────────────────────────────────────────────────────────── basket ──

type Line = { code: string; qty: number; name: string };

function parseBasket(raw: string | null): Line[] {
  return (raw || "").split(",").filter(Boolean).map((part) => {
    const [code, qty, ...name] = part.split(":");
    return { code, qty: Number(qty) || 1, name: decodeURIComponent(name.join(":") || "") };
  });
}

function serializeBasket(lines: Line[]): string | null {
  return lines.length ? lines.map((l) => `${l.code}:${l.qty}:${encodeURIComponent(l.name)}`).join(",") : null;
}

function BasketTab({ params, patch }: { params: URLSearchParams; patch: (p: Record<string, string | null>) => void }) {
  const lines = useMemo(() => parseBasket(params.get("basket")), [params]);
  const city = params.get("city") || "";
  const [cityInput, setCityInput] = useState(city);
  const [stores, setStores] = useState<PriceBasketStore[] | null>(null);
  const [perChain, setPerChain] = useState<{ chain: string; chain_name: string; store_name: string | null; total: number; items_found: number }[]>([]);
  const [compared, setCompared] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const setLines = (next: Line[]) => patch({ basket: serializeBasket(next) });

  useEffect(() => {
    if (!lines.length || !city) { setStores(null); return; }
    setLoading(true); setError(null);
    pricesApi.basket(lines.map((l) => ({ item_code: l.code, quantity: l.qty })), city, 15)
      .then((r) => { setStores(r.stores); setPerChain(r.best_per_chain); setCompared(r.stores_compared); })
      .catch((e) => setError(String(e.message || e)))
      .finally(() => setLoading(false));
  }, [lines, city]);

  return (
    <>
      <ProductSearch initial="" actionLabel="+ לסל" onQuery={() => undefined}
                     onPick={(p) => {
                       if (lines.some((l) => l.code === p.item_code) || lines.length >= 40) return;
                       setLines([...lines, { code: p.item_code, qty: 1, name: p.names[0] ?? p.item_code }]);
                     }} />

      <section style={{ marginTop: "1.2rem" }}>
        <h2 style={{ fontSize: "1.1rem", margin: "0 0 0.5rem" }}>הסל ({lines.length})</h2>
        {lines.length === 0 && <div className="text-sm text-muted">חפשו מוצרים והוסיפו אותם לסל. הסל נשמר בכתובת הדף, כך שאפשר לשתף אותו.</div>}
        {lines.length > 0 && (
          <ul style={{ listStyle: "none", padding: 0, margin: 0 }}>
            {lines.map((l, i) => (
              <li key={l.code} className="flex text-sm" style={{ gap: "0.5rem", alignItems: "center", padding: "0.2rem 0" }}>
                <input type="number" min={1} max={99} value={l.qty} aria-label={`כמות ${l.name}`}
                       onChange={(e) => setLines(lines.map((x, j) => j === i ? { ...x, qty: Math.max(1, Number(e.target.value) || 1) } : x))}
                       style={{ width: 56, padding: "0.2rem 0.3rem" }} />
                <span style={{ flex: 1 }}>{l.name} <span className="text-muted"><Ltr>{l.code}</Ltr></span></span>
                <button type="button" style={btn} aria-label={`הסרת ${l.name}`}
                        onClick={() => setLines(lines.filter((_, j) => j !== i))}>✕</button>
              </li>
            ))}
          </ul>
        )}
        <form className="flex" style={{ gap: "0.5rem", alignItems: "flex-end", marginTop: "0.7rem", flexWrap: "wrap" }}
              onSubmit={(e) => { e.preventDefault(); patch({ city: cityInput.trim() || null }); }}>
          <label className="text-sm">
            עיר
            <br />
            <input value={cityInput} onChange={(e) => setCityInput(e.target.value)} placeholder="למשל: חיפה"
                   style={{ padding: "0.35rem 0.5rem", width: 170 }} />
          </label>
          <button type="submit" style={primaryBtn} disabled={!lines.length}>השוואת הסל</button>
        </form>
      </section>

      {loading && <div className="text-sm text-muted" style={{ marginTop: "0.6rem" }}>מחשב את הסל בכל סניף…</div>}
      <ErrorLine error={error} />
      {stores && (
        <section style={{ marginTop: "1rem" }} aria-live="polite">
          <div className="text-sm text-muted" style={{ marginBottom: "0.4rem" }}>
            הושוו <Ltr>{compared}</Ltr> סניפים ב{city}. הדירוג הוא קודם לפי כמה מפריטי הסל יש בסניף ורק אחר כך לפי הסכום —
            סניף זול כי חסרים בו פריטים אינו הזול.
          </div>
          <div tabIndex={0} role="region" aria-label="דירוג הסניפים" className="scroll-region" style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", fontSize: "0.86rem", borderCollapse: "collapse" }}>
              <thead>
                <tr style={{ color: "var(--text-muted)" }}>
                  {["#", "רשת", "סניף", "סה״כ", "פריטים שנמצאו", "חסרים", "נכון ל"].map((h) => <th key={h} scope="col" style={cell}>{h}</th>)}
                </tr>
              </thead>
              <tbody>
                {stores.map((s, i) => (
                  <tr key={`${s.chain}-${s.store_id}`} style={{ borderTop: "1px solid var(--border)" }}>
                    <td style={cell}><Ltr>{i + 1}</Ltr></td>
                    <td style={cell}>{s.chain_name}</td>
                    <td style={cell}>{s.store_name ?? s.store_id}{s.address ? <span className="text-muted"> · {s.address}</span> : null}</td>
                    <td style={{ ...cell, fontWeight: 700 }}><Ltr>{money(s.total)}</Ltr></td>
                    <td style={cell}><Ltr>{s.items_found}/{lines.length}</Ltr></td>
                    <td style={cell}>{s.items_missing.length
                      ? s.items_missing.map((c) => lines.find((l) => l.code.replace(/^0+/, "") === c)?.name ?? c).join(", ")
                      : "—"}</td>
                    <td style={cell}><Ltr>{heDate(s.as_of)}</Ltr></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {perChain.length > 0 && (
            <>
              <h3 style={{ fontSize: "1rem", margin: "1rem 0 0.4rem" }}>הסניף הזול בכל רשת</h3>
              <ul className="text-sm" style={{ margin: 0, paddingInlineStart: "1.2rem", lineHeight: 1.8 }}>
                {perChain.map((c) => (
                  <li key={c.chain}>{c.chain_name}: <strong><Ltr>{money(c.total)}</Ltr></strong> ({c.store_name ?? "—"}, <Ltr>{c.items_found}/{lines.length}</Ltr> פריטים)</li>
                ))}
              </ul>
            </>
          )}
        </section>
      )}
    </>
  );
}

// ───────────────────────────────────────────────────────── uniform table ──

const TABLE_PARAMS = ["item_code", "q", "chain", "city", "store_id", "promotion_id", "date", "current", "order", "offset"];
const PAGE = 100;

function TableTab({ params, patch, chains }: {
  params: URLSearchParams; patch: (p: Record<string, string | null>) => void; chains: PriceChain[];
}) {
  const [specs, setSpecs] = useState<PriceTableSpec[]>([]);
  const t = params.get("t") || "prices_market";
  const spec = specs.find((s) => s.name === t);
  const query = useMemo(() => {
    const out: Record<string, string> = { limit: String(PAGE) };
    for (const k of TABLE_PARAMS) {
      const v = params.get(k);
      if (v) out[k] = v;
    }
    return out;
  }, [params]);
  const [result, setResult] = useState<PriceTableResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [draft, setDraft] = useState<Record<string, string>>(query);

  useEffect(() => { pricesApi.tables().then((r) => setSpecs(r.tables)).catch(() => undefined); }, []);
  useEffect(() => { setDraft(query); }, [query]);

  const needsFilter = t === "prices_market" && !(query.item_code || query.q || query.store_id);
  useEffect(() => {
    if (needsFilter) { setResult(null); return; }
    setLoading(true); setError(null);
    pricesApi.table(t, query)
      .then(setResult)
      .catch((e) => { setError(String(e.message || e)); setResult(null); })
      .finally(() => setLoading(false));
  }, [t, query, needsFilter]);

  const has = (c: string) => !spec || spec.columns.some((x) => x.name === c);
  const offset = Number(query.offset || 0);
  const field = (key: string, label: string, width = 130, placeholder = "") => (
    <label className="text-sm" key={key}>
      {label}
      <br />
      <input value={draft[key] ?? ""} placeholder={placeholder}
             onChange={(e) => setDraft({ ...draft, [key]: e.target.value })}
             style={{ padding: "0.35rem 0.5rem", width }} />
    </label>
  );

  return (
    <>
      <p className="text-sm" style={{ lineHeight: 1.7, marginTop: 0 }}>
        כל רשת מפרסמת בפורמט שלה ונשמרת כמאגר נפרד. כאן כולן הן <strong>טבלה אחת עם סכמה אחת</strong>:
        שש טבלאות אחידות (מחירים, מוצרים, סניפים, מבצעים, פריטי מבצעים, כיסוי יומי) שמאחדות את כל הרשתות.
        אותן טבלאות זמינות בשמן בקונסולת ה-<a href="/data">SQL</a>, ב-<a href="/api#prices">API</a> וב-MCP.
      </p>

      <div className="flex" style={{ gap: "0.35rem", flexWrap: "wrap", marginBottom: "0.7rem" }} role="group" aria-label="בחירת טבלה">
        {(specs.length ? specs.map((s) => [s.name, s.title]) : [["prices_market", "מחירים — כל הרשתות"]]).map(([name, title]) => (
          <button key={name} type="button" aria-pressed={t === name}
                  onClick={() => patch({ t: name === "prices_market" ? null : name, offset: null, order: null })}
                  style={{ ...btn, background: t === name ? "var(--primary)" : "none", color: t === name ? "#fff" : undefined }}>
            {title.split(" — ")[0]} <span style={{ opacity: 0.75, fontSize: "0.75rem" }}><Ltr>{name}</Ltr></span>
          </button>
        ))}
      </div>
      {spec && <div className="text-sm text-muted" style={{ marginBottom: "0.6rem" }}>{spec.description}</div>}

      <form className="flex" style={{ gap: "0.5rem", flexWrap: "wrap", alignItems: "flex-end", marginBottom: "0.7rem" }}
            onSubmit={(e) => {
              e.preventDefault();
              const next: Record<string, string | null> = { offset: null };
              for (const k of TABLE_PARAMS) if (k !== "offset") next[k] = (draft[k] ?? "").trim() || null;
              patch(next);
            }}>
        {has("item_code") && field("item_code", "ברקוד", 150, "כמה, מופרדים בפסיק")}
        {t !== "prices_coverage" && field("q", "חיפוש חופשי", 150, t === "prices_market" ? "שם מוצר" : "")}
        <label className="text-sm">
          רשת
          <br />
          <select value={draft.chain ?? ""} onChange={(e) => setDraft({ ...draft, chain: e.target.value })}
                  style={{ padding: "0.35rem 0.5rem", maxWidth: 200 }}>
            <option value="">כל הרשתות</option>
            {chains.map((c) => <option key={c.chain} value={c.chain}>{c.name}</option>)}
          </select>
        </label>
        {has("city_code") && field("city", "עיר", 110)}
        {has("store_id") && field("store_id", "מספר סניף", 90)}
        {has("promotion_id") && t !== "prices_market" && field("promotion_id", "מזהה מבצע", 110)}
        {has("is_current") && (
          <>
            <label className="text-sm">
              בתוקף
              <br />
              <select value={draft.current ?? "true"} onChange={(e) => setDraft({ ...draft, current: e.target.value === "true" ? "" : e.target.value })}
                      style={{ padding: "0.35rem 0.5rem" }}>
                <option value="true">רק מה שבתוקף עכשיו</option>
                <option value="false">כל ההיסטוריה</option>
              </select>
            </label>
            <label className="text-sm">
              בתוקף ביום
              <br />
              <input type="date" value={draft.date ?? ""} onChange={(e) => setDraft({ ...draft, date: e.target.value })}
                     style={{ padding: "0.3rem 0.4rem" }} />
            </label>
          </>
        )}
        {field("order", "מיון", 130, t === "prices_market" ? "item_price" : "-first_seen_date")}
        <button type="submit" style={primaryBtn}>הצגה</button>
      </form>

      {needsFilter && (
        <div className="text-sm" style={{ padding: "0.6rem 0.8rem", border: "1px dashed var(--border)", borderRadius: 8 }}>
          טבלת המחירים מכילה מיליוני מצבי מחיר — ציינו ברקוד, שם מוצר, או מספר סניף ורשת.
          לדוגמה: <a href="/projects/prices?tab=table&item_code=7290000066134&city=חיפה">ביסלי פלאפל בחיפה</a>.
        </div>
      )}
      {loading && <div className="text-sm text-muted">טוען…</div>}
      <ErrorLine error={error} />

      {result && (
        <>
          <div className="text-sm text-muted" style={{ margin: "0.4rem 0", display: "flex", gap: "0.8rem", flexWrap: "wrap" }}>
            <span>{result.count === 0 ? "אין שורות לסינון הזה." : <>שורות <Ltr>{offset + 1}</Ltr>–<Ltr>{offset + result.count}</Ltr>{result.has_more ? " (יש עוד)" : ""}</>}</span>
            <span><Ltr>{result.elapsed_ms}ms</Ltr></span>
            {result.resolved_item_codes && result.resolved_item_codes.length > 0 &&
              <span title={result.resolved_item_codes.join(", ")}>החיפוש תורגם ל-<Ltr>{result.resolved_item_codes.length}</Ltr> ברקודים</span>}
            <a href={pricesApi.tableUrl(t, query, "csv")}>CSV ↓</a>
            <a href={pricesApi.tableUrl(t, query)} target="_blank" rel="noopener noreferrer">
              קריאת ה-API ↗<span className="sr-only"> (נפתח בחלון חדש)</span>
            </a>
            {result.row_url && (
              <a href={result.row_url} target="_blank" rel="noopener noreferrer" title={result.console_sql ?? ""}>
                אותה שאילתה ב-/data ↗<span className="sr-only"> (נפתח בחלון חדש)</span>
              </a>
            )}
          </div>
          {result.rows.length > 0 && (
            <div tabIndex={0} role="region" aria-label="הטבלה האחידה" className="scroll-region" style={{ overflowX: "auto" }}>
              <table style={{ fontSize: "0.8rem", borderCollapse: "collapse", minWidth: "100%" }}>
                <thead>
                  <tr style={{ color: "var(--text-muted)" }}>
                    {result.columns.map((c) => (
                      <th key={c} scope="col" style={{ ...cell, whiteSpace: "nowrap" }}
                          title={spec?.columns.find((x) => x.name === c)?.description}>
                        <Ltr>{c}</Ltr>
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {result.rows.map((r, i) => (
                    <tr key={i} style={{ borderTop: "1px solid var(--border)" }}>
                      {result.columns.map((c) => {
                        const v = r[c];
                        return <td key={c} style={{ ...cell, whiteSpace: "nowrap", maxWidth: 260, overflow: "hidden", textOverflow: "ellipsis" }}>
                          {v === null || v === "" ? <span className="text-muted">—</span>
                            : typeof v === "boolean" ? (v ? "✓" : "✗")
                            : c === "dataset_id" ? <a href={`/versions/${v}`}>גרסאות</a>
                            : String(v)}
                        </td>;
                      })}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
          {(offset > 0 || result.has_more) && (
            <div className="flex" style={{ gap: "0.5rem", marginTop: "0.6rem" }}>
              <button type="button" style={btn} disabled={offset === 0}
                      onClick={() => patch({ offset: offset - PAGE > 0 ? String(offset - PAGE) : null })}>הקודם</button>
              <button type="button" style={btn} disabled={!result.has_more}
                      onClick={() => patch({ offset: String(offset + PAGE) })}>הבא</button>
            </div>
          )}
        </>
      )}

      {spec && (
        <details style={{ marginTop: "1rem" }}>
          <summary className="text-sm" style={{ cursor: "pointer" }}>עמודות הטבלה <Ltr>{spec.name}</Ltr></summary>
          <table style={{ fontSize: "0.8rem", borderCollapse: "collapse", marginTop: "0.4rem" }}>
            <tbody>
              {spec.columns.map((c) => (
                <tr key={c.name} style={{ borderTop: "1px solid var(--border)" }}>
                  <td style={cell}><Ltr><code>{c.name}</code></Ltr></td>
                  <td style={cell} className="text-muted"><Ltr>{c.type}</Ltr></td>
                  <td style={cell}>{c.description}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </details>
      )}
    </>
  );
}

// ──────────────────────────────────────────────────────────────── chains ──

function ChainsTab({ chains, error }: { chains: PriceChain[] | null; error: string | null }) {
  const sorted = useMemo(() => [...(chains ?? [])].sort((a, b) => (b.stores ?? 0) - (a.stores ?? 0)), [chains]);
  return (
    <>
      <ErrorLine error={error} />
      {!chains && !error && <div className="text-sm text-muted">טוען…</div>}
      {chains && (
        <div tabIndex={0} role="region" aria-label="הרשתות" className="scroll-region" style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", fontSize: "0.86rem", borderCollapse: "collapse" }}>
            <thead>
              <tr style={{ color: "var(--text-muted)" }}>
                {["רשת", "מפתח", "סניפים", "הקובץ האחרון", "מקור", "היסטוריה"].map((h) => <th key={h} scope="col" style={cell}>{h}</th>)}
              </tr>
            </thead>
            <tbody>
              {sorted.map((c) => (
                <tr key={c.chain} style={{ borderTop: "1px solid var(--border)" }}>
                  <td style={cell}>{c.name}</td>
                  <td style={cell}><Ltr><code>{c.chain}</code></Ltr>{c.account && c.account !== c.name ? <span className="text-muted"> · {c.account}</span> : null}</td>
                  <td style={cell}><Ltr>{c.stores ?? "—"}</Ltr></td>
                  <td style={cell}><Ltr>{heDate(c.latest_snapshot)}</Ltr></td>
                  <td style={cell}>{c.source_url
                    ? <a href={c.source_url} target="_blank" rel="noopener noreferrer">אתר הרשת ↗<span className="sr-only"> (נפתח בחלון חדש)</span></a>
                    : "—"}</td>
                  <td style={cell}><a href={`/versions/${c.dataset_id}`}>גרסאות</a></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </>
  );
}

// ────────────────────────────────────────────────────────────────── page ──

export default function PricesPage() {
  useDocumentTitle("שקיפות מחירים");
  const [params, setParams] = useSearchParams();
  const urlTab = params.get("tab") as Tab | null;
  const tab: Tab = urlTab && TABS.some(([id]) => id === urlTab) ? urlTab : "compare";

  const [chains, setChains] = useState<PriceChain[] | null>(null);
  const [chainsError, setChainsError] = useState<string | null>(null);
  const [caveats, setCaveats] = useState<string[]>([]);

  useEffect(() => {
    pricesApi.chains()
      .then((r) => { setChains(r.chains); setCaveats(r.caveats); })
      .catch((e) => setChainsError(String(e.message || e)));
  }, []);

  const patch = useCallback((changes: Record<string, string | null>) => {
    setParams((prev) => {
      const next = new URLSearchParams(prev);
      for (const [k, v] of Object.entries(changes)) {
        if (v === null || v === "") next.delete(k); else next.set(k, v);
      }
      return next;
    }, { replace: false });
  }, [setParams]);

  const totalStores = (chains ?? []).reduce((n, c) => n + (c.stores ?? 0), 0);
  const latest = (chains ?? []).map((c) => c.latest_snapshot ?? "").sort().pop();

  return (
    <div>
      <div className="processed-banner" role="note">
        <div className="container">
          <span className="processed-banner-badge">מקור ראשוני</span>
          <span className="processed-banner-text">
            המחירים כאן הם <strong>מה שהרשתות עצמן פרסמו</strong> לפי חוק קידום התחרות בענף המזון —
            לא תוקנו ולא אומתו מול הקופה. רק נאספו למקום אחד שאפשר לתשאל.
          </span>
        </div>
      </div>

      <div className="container mt-3">
        <div className="page-header" style={{ marginBottom: "0.75rem" }}>
          <h1 style={{ margin: 0 }}>שקיפות מחירים</h1>
          <div className="text-sm text-muted" style={{ marginTop: "0.35rem", lineHeight: 1.7 }}>
            כל רשת מזון גדולה מחויבת לפרסם את סניפיה, את מחיר כל מוצר בכל סניף ואת המבצעים — כל אחת באתר
            שלה ובפורמט שלה. כאן הן שוק אחד: איפה מוצר זול יותר, כמה עולה סל בכל סניף בעיר, וטבלה אחידה
            אחת לכל הרשתות שאפשר לתשאל, להוריד ולחבר ב-API.
            {chains && (
              <div style={{ marginTop: "0.4rem" }}>
                <Ltr>{chains.length}</Ltr> רשתות · <Ltr>{totalStores.toLocaleString("he-IL")}</Ltr> סניפים ·
                עדכון אחרון <Ltr>{heDate(latest)}</Ltr> ·{" "}
                <a href="/sources/prices">המקור</a> · <a href="/api#prices">API</a> ·{" "}
                <a href="/api#mcp-prices">MCP</a>
              </div>
            )}
          </div>
        </div>

        <div className="flex" role="tablist" style={{ gap: "0.3rem", borderBottom: "2px solid var(--border)", marginBottom: "1rem", flexWrap: "wrap" }}>
          {TABS.map(([id, label]) => (
            <button key={id} type="button" role="tab" aria-selected={tab === id}
                    onClick={() => patch({ tab: id === "compare" ? null : id })}
                    style={{
                      padding: "0.5rem 1.05rem", border: "none", cursor: "pointer", background: "none",
                      fontSize: "0.95rem", fontWeight: tab === id ? 700 : 500,
                      color: tab === id ? "var(--primary)" : "var(--text-muted)",
                      borderBottom: tab === id ? "3px solid var(--primary)" : "3px solid transparent",
                      marginBottom: -2,
                    }}>
              {label}
            </button>
          ))}
        </div>

        {tab === "compare" && <CompareTab params={params} patch={patch} />}
        {tab === "basket" && <BasketTab params={params} patch={patch} />}
        {tab === "table" && <TableTab params={params} patch={patch} chains={chains ?? []} />}
        {tab === "chains" && <ChainsTab chains={chains} error={chainsError} />}

        {caveats.length > 0 && (
          <details style={{ margin: "1.5rem 0 1rem" }}>
            <summary className="text-sm" style={{ cursor: "pointer" }}>מה חשוב לדעת על הנתונים</summary>
            <ul className="text-sm" style={{ lineHeight: 1.7 }}>
              {caveats.map((c) => <li key={c}>{c}</li>)}
            </ul>
          </details>
        )}
      </div>
    </div>
  );
}
