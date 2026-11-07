/**
 * נדל"ן לעם — what the general search box found besides properties: how the
 * text was read, RMI tenders, plans and streets. The properties themselves are
 * rendered by the page with the same NadlanResultCard every other mode uses.
 */
import type { NadlanReading, NadlanSearch, NadlanStreet } from "../../api/client";
import NadlanTile from "./NadlanTile";

const KIND_LABEL: Record<NadlanReading["kind"], string> = {
  point: "נקודה",
  gush_helka: "גוש/חלקה",
  gush: "גוש",
  zip: "מיקוד",
  address: "כתובת",
  tender: 'מכרז רמ"י',
  plan: "מספר תכנית",
  text: "טקסט חופשי",
};

function readingText(r: NadlanReading): string {
  const p = r.parsed as Record<string, unknown>;
  switch (r.kind) {
    case "point": return `${p.lat}, ${p.lon}`;
    case "gush_helka": return `גוש ${p.gush} חלקה ${p.helka}`;
    case "gush": return `גוש ${p.gush}`;
    case "zip": return String(p.zip);
    case "address": return [p.street, p.number, p.city].filter(Boolean).join(" ");
    case "tender": return String(p.tender_number);
    case "plan": return String(p.number);
    default: return String(p.text ?? "");
  }
}

const CHIP: React.CSSProperties = {
  padding: "0.2rem 0.65rem", fontSize: "0.82rem", cursor: "pointer", minHeight: 0,
  border: "1px solid var(--border)", borderRadius: 6, background: "var(--surface)",
};

export default function NadlanSearchResults({ res, onParcel, onStreet }: {
  res: NadlanSearch;
  onParcel: (gush: number, helka: number) => void;
  onStreet: (s: NadlanStreet) => void;
}) {
  const readings = res.interpretations;
  const nothing = res.properties.length === 0 && res.tenders.length === 0
    && res.plans.length === 0 && res.streets.length === 0;
  const errs = Object.keys(res.errors);

  return (
    <div>
      {readings.length > 0 && (
        <div className="text-sm" style={{ marginBottom: "0.8rem", lineHeight: 1.9 }}>
          <span className="text-muted">החיפוש נקרא כ: </span>
          {readings.map((r, i) => (
            <span key={i} style={{
              display: "inline-block", marginInlineEnd: "0.35rem", padding: "0.05rem 0.55rem",
              borderRadius: 999, background: "var(--surface-2)", border: "1px solid var(--border)",
            }}>
              {KIND_LABEL[r.kind]}: <strong>{readingText(r)}</strong>
            </span>
          ))}
        </div>
      )}

      {nothing && (
        <div className="text-sm text-muted" style={{ marginBottom: "1rem" }}>
          לא נמצא דבר באף אחד מהמאגרים. נסו כתובת עם שם היישוב (למשל "המגיד 14 ירושלים"),
          גוש/חלקה ("6638/120"), מיקוד, מספר מכרז ("216/2020") או מספר תכנית ("תא/3598").
        </div>
      )}
      {errs.length > 0 && (
        <div className="text-sm" style={{ color: "var(--danger)", marginBottom: "0.8rem" }}>
          חלק מהחיפושים לא הסתיימו ({errs.join(", ")}). התוצאות האחרות מוצגות.
        </div>
      )}

      {(res.tenders.length > 0 || res.plans.length > 0 || res.streets.length > 0) && (
        <div className="nadlan-tiles" style={{ marginTop: 0, marginBottom: "1.5rem" }}>
          {res.tenders.map((t) => (
            <NadlanTile
              key={t.tender_id}
              title={`מכרז ${t.tender_number}${t.settlement ? ` · ${t.settlement}` : ""}`}
              badge={`${t.lots.toLocaleString("he-IL")} מתחמים`}
              label='מכרזי רמ"י'
              links={[{ href: t.tender_url, label: 'דף המכרז באתר רמ"י' }]}
            >
              <div>{[t.tender_type, t.purpose].filter(Boolean).join(" · ")}</div>
              {t.status && <div className="text-muted">{t.status}{t.committee_date ? ` · ועדה ${t.committee_date}` : ""}</div>}
              {t.location && <div className="text-muted">מיקום: {t.location}</div>}
              <div>
                {t.units > 0 && <>{t.units.toLocaleString("he-IL")} יח"ד · </>}
                {t.lots_won.toLocaleString("he-IL")} נמכרו
                {t.winning_total != null && <> · ₪{t.winning_total.toLocaleString("he-IL")}</>}
              </div>
              {t.winners.length > 0 && <div className="text-muted">זוכים: {t.winners.join(", ")}</div>}
              <div className="flex" style={{ gap: "0.35rem", flexWrap: "wrap", marginTop: "0.4rem" }}>
                {t.parcels.map((p) => (
                  <button key={`${p.gush}-${p.helka}`} type="button" style={CHIP}
                          onClick={() => onParcel(p.gush, p.helka)}>
                    גוש {p.gush} חלקה {p.helka}
                  </button>
                ))}
                {t.parcels.length === 0 && (
                  <span className="text-muted">רמ"י לא פרסמה גוש וחלקה למכרז הזה.</span>
                )}
              </div>
            </NadlanTile>
          ))}

          {res.plans.map((p, i) => (
            <NadlanTile
              key={`${p.source}-${p.number}-${i}`}
              title={p.number}
              badge={`${p.parcel_count.toLocaleString("he-IL")} חלקות`}
              label={p.source_title}
              links={p.url ? [{ href: p.url, label: 'התכנית במבא"ת' }] : []}
            >
              {p.name && <div className="nadlan-tile-clamp">{p.name}</div>}
              {(p.settlement || p.status) && (
                <div className="text-muted">{[p.settlement, p.status].filter(Boolean).join(" · ")}</div>
              )}
              <div className="flex" style={{ gap: "0.35rem", flexWrap: "wrap", marginTop: "0.4rem" }}>
                {p.parcels.slice(0, 6).map((x) => (
                  <button key={`${x.gush}-${x.suffix}-${x.helka}`} type="button" style={CHIP}
                          onClick={() => onParcel(x.gush, x.helka)}>
                    {x.gush}/{x.helka}
                  </button>
                ))}
                {p.parcel_count > 6 && (
                  <span className="text-muted" style={{ alignSelf: "center" }}>
                    ועוד {(p.parcel_count - 6).toLocaleString("he-IL")}
                  </span>
                )}
              </div>
            </NadlanTile>
          ))}

          {res.streets.length > 0 && (
            <NadlanTile title="רחובות" badge={res.streets.length} label="רשימת הרחובות">
              <div className="flex" style={{ gap: "0.35rem", flexWrap: "wrap" }}>
                {res.streets.map((s) => (
                  <button key={s.street_key} type="button" style={CHIP} onClick={() => onStreet(s)}>
                    {s.name}{s.settlement_name ? `, ${s.settlement_name}` : ""}
                  </button>
                ))}
              </div>
            </NadlanTile>
          )}
        </div>
      )}

      {res.properties.length > 0 && (
        <h2 style={{ fontSize: "1.05rem", margin: "0 0 0.6rem" }}>
          נכסים <span className="text-muted" style={{ fontWeight: 400 }}>({res.properties.length})</span>
        </h2>
      )}
    </div>
  );
}
