import { useEffect, useState } from "react";
import { ocal, OcalOwner } from "../../api/client";

// One fetch per page load: the list is ~300 rows and every Ocal tab uses it.
type OwnersResponse = { data: OcalOwner[]; expenses_enabled: boolean };
let ownersPromise: Promise<OwnersResponse> | null = null;

function loadOwnersResponse(): Promise<OwnersResponse> {
  if (!ownersPromise) {
    ownersPromise = ocal.owners().catch((e) => {
      ownersPromise = null;
      throw e;
    });
  }
  return ownersPromise;
}

export function loadOwners(): Promise<OcalOwner[]> {
  return loadOwnersResponse().then((r) => r.data);
}

/** The owners, and whether the contact-with-the-voter expenses layer is on. */
export function useOcalOwnersInfo(): { owners: OcalOwner[]; expensesEnabled: boolean } {
  const [info, setInfo] = useState<{ owners: OcalOwner[]; expensesEnabled: boolean }>({ owners: [], expensesEnabled: false });
  useEffect(() => {
    let live = true;
    loadOwnersResponse()
      .then((r) => { if (live) setInfo({ owners: r.data, expensesEnabled: r.expenses_enabled }); })
      .catch(() => {});
    return () => { live = false; };
  }, []);
  return info;
}

export function useOcalOwners(): OcalOwner[] {
  return useOcalOwnersInfo().owners;
}

const byLabel = (a: OcalOwner, b: OcalOwner) => a.label.localeCompare(b.label, "he");

/**
 * Diary-owner picker: people first, then the offices of diaries that name
 * nobody. MKs that have only contact-with-the-public expenses (no diary) are
 * left out unless ``includeExpenseOnly`` — a filter on diaries would match none.
 */
export default function OwnerSelect({
  value,
  onChange,
  includeExpenseOnly = false,
  style,
}: {
  value: string;
  onChange: (key: string) => void;
  includeExpenseOnly?: boolean;
  style?: React.CSSProperties;
}) {
  const owners = useOcalOwners();
  const shown = owners.filter((o) => includeExpenseOnly || o.diary_count > 0);
  const people = shown.filter((o) => o.kind === "person").sort(byLabel);
  const offices = shown.filter((o) => o.kind === "subject").sort(byLabel);
  return (
    <select
      aria-label="סינון לפי בעל היומן"
      value={value}
      onChange={(e) => onChange(e.target.value)}
      style={{ padding: "0.4rem 0.6rem", border: "1px solid var(--border)", borderRadius: 4, fontSize: "0.9rem", maxWidth: 280, ...style }}
    >
      <option value="">כל בעלי היומנים ({people.length})</option>
      {value && !shown.some((o) => o.key === value) && <option value={value}>{value.replace(/^[ps]:/, "")}</option>}
      <optgroup label="אנשים">
        {people.map((o) => (
          <option key={o.key} value={o.key}>
            {o.label}{o.diary_count ? ` (${o.diary_count})` : ""}
          </option>
        ))}
      </optgroup>
      {offices.length > 0 && (
        <optgroup label="יומני תפקיד (ללא שם)">
          {offices.map((o) => (
            <option key={o.key} value={o.key}>{o.label} ({o.diary_count})</option>
          ))}
        </optgroup>
      )}
    </select>
  );
}
