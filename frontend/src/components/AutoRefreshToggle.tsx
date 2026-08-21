/**
 * The control that stops a region from re-fetching under the reader
 * (WCAG 2.2.2 Pause, Stop, Hide — and 2.2.4 Interruptions, which wants the
 * choice to stick rather than being re-made on every visit).
 *
 * The preference is global on purpose: "stop updating things at me" is a
 * statement about the app, not about one panel, so pausing here pauses the
 * activity log, the scrape banner and the navbar counter together.
 */
export function AutoRefreshToggle(props: {
  paused: boolean;
  onToggle: () => void;
  seconds: number;
}) {
  const label = props.paused
    ? "עדכון אוטומטי מושהה — הפעלה מחדש"
    : `התוכן מתעדכן אוטומטית כל ${props.seconds} שניות — השהיה`;
  return (
    <button
      type="button"
      className="autorefresh-toggle"
      aria-pressed={props.paused}
      onClick={props.onToggle}
      title={label}
    >
      <span aria-hidden="true">{props.paused ? "▶" : "⏸"}</span>
      <span>{props.paused ? "עדכון אוטומטי מושהה" : "השהיית עדכון אוטומטי"}</span>
    </button>
  );
}

export default AutoRefreshToggle;
