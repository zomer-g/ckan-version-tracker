/**
 * HTTP layer: talks to OVER's connector API (app/api/connector.py).
 *
 * There is deliberately NO client-side secret here: the script is shared
 * "anyone with link: Viewer" (required for by-link connector distribution),
 * and viewers can see Script Properties — so nothing stored in this project
 * can be secret. The backend instead recognizes connector traffic by
 * Google's egress IP ranges (where Apps Script always runs).
 */

var BASE = 'https://over.org.il';

function _userError(text) {
  DataStudioApp.createCommunityConnector()
    .newUserError()
    .setText(text)
    .throwException();
}

/** Parse an OVER error response body into a user-facing message. */
function _errorDetail(response) {
  try {
    var body = JSON.parse(response.getContentText());
    // FastAPI errors carry {detail}; the byte-budget 429 carries {message}.
    return body.detail || body.message || ('HTTP ' + response.getResponseCode());
  } catch (e) {
    return 'HTTP ' + response.getResponseCode();
  }
}

/**
 * Run a read-only SELECT on OVER. Returns the backend envelope:
 * {columns: [name], fields: [{id, type}], rows: [{col: value}],
 *  truncated: bool, row_count: int}
 */
function apiSql(sql, maxRows) {
  var response = UrlFetchApp.fetch(BASE + '/api/connector/sql', {
    method: 'post',
    contentType: 'application/json',
    payload: JSON.stringify({ sql: sql, max_rows: maxRows }),
    muteHttpExceptions: true,
  });
  if (response.getResponseCode() !== 200) {
    _userError('שגיאה מהשרת של גרסאות לעם: ' + _errorDetail(response));
  }
  return JSON.parse(response.getContentText());
}

/**
 * Dev-only smoke test — run from the Apps Script editor. Deliberately does
 * a RAW fetch (no _userError): a CommunityConnector userError thrown
 * outside a real Data Studio request still shows the run as "Completed",
 * which reads as false success. Here the log always tells the truth:
 *   HTTP 200 + {"columns":["x"],...} → end-to-end OK
 *   HTTP 401 → the server did not classify this as Google-infra traffic
 *   HTTP 503 → CONNECTOR_API_KEY not set on the server (feature off)
 *   HTML/403 → Cloudflare challenging Google IPs → WAF skip rule needed
 */
function smoke() {
  var response = UrlFetchApp.fetch(BASE + '/api/connector/sql', {
    method: 'post',
    contentType: 'application/json',
    payload: JSON.stringify({ sql: 'SELECT 1 AS x', max_rows: 1 }),
    muteHttpExceptions: true,
  });
  Logger.log('HTTP ' + response.getResponseCode());
  Logger.log(response.getContentText().slice(0, 500));
}

/** The trimmed table catalog for the config dropdown. */
function fetchTables() {
  var response = UrlFetchApp.fetch(BASE + '/api/connector/tables', {
    muteHttpExceptions: true,
  });
  if (response.getResponseCode() !== 200) {
    _userError('לא ניתן לטעון את רשימת הטבלאות: ' + _errorDetail(response));
  }
  return JSON.parse(response.getContentText()).tables || [];
}
