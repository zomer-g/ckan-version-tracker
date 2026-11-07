import { describe, expect, it } from "vitest";
import { safeHref } from "./safeHref";

describe("safeHref", () => {
  it("keeps web links and same-site paths", () => {
    expect(safeHref("https://www.gov.il/he/x")).toBe("https://www.gov.il/he/x");
    expect(safeHref("http://a.b/c")).toBe("http://a.b/c");
    expect(safeHref("/datasets/1")).toBe("/datasets/1");
    expect(safeHref("mailto:a@b.c")).toBe("mailto:a@b.c");
  });

  it("drops script and data URLs and protocol-relative ones", () => {
    expect(safeHref("javascript://main.knesset.gov.il/x%0aalert(1)")).toBeUndefined();
    expect(safeHref(" JavaScript:alert(1)")).toBeUndefined();
    expect(safeHref("data:text/html,<script>1</script>")).toBeUndefined();
    expect(safeHref("vbscript:x")).toBeUndefined();
    expect(safeHref("//evil.example/x")).toBeUndefined();
    expect(safeHref("")).toBeUndefined();
    expect(safeHref(null)).toBeUndefined();
  });
});
