import { useEffect, useState } from "react";
import { decisionAnalysis, DecisionAnalysisSummary } from "../api/client";

// The list of PUBLISHED decision analyses, used by the navbar and the rationale
// page to decide whether the link exists at all. While nothing is published the
// endpoint returns [], so an unpublished draft leaves no trace in the UI.
//
// The promise is cached at module level: the navbar renders on every page and
// this must not become a request per navigation. A failure resolves to [] (the
// link simply doesn't appear) — this is decoration, never a blocking error.
let cached: Promise<DecisionAnalysisSummary[]> | null = null;

function load(): Promise<DecisionAnalysisSummary[]> {
  if (!cached) cached = decisionAnalysis.list().catch(() => []);
  return cached;
}

/** Forget the cached list — call after an admin publishes/unpublishes. */
export function invalidatePublishedDecisions() {
  cached = null;
}

export function usePublishedDecisions(): DecisionAnalysisSummary[] {
  const [items, setItems] = useState<DecisionAnalysisSummary[]>([]);

  useEffect(() => {
    let alive = true;
    load().then((list) => {
      if (alive) setItems(list);
    });
    return () => {
      alive = false;
    };
  }, []);

  return items;
}
