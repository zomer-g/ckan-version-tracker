/**
 * HTTP layer: talks to OVER's key-gated connector API (app/api/connector.py).
 *
 * The shared secret lives in Script Properties (key OVER_CONNECTOR_KEY), never
 * in source — set it once in the Apps Script editor (Project Settings →
 * Script Properties). It must equal the backend's CONNECTOR_API_KEY env.
 */

var BASE = 'https://over.org.il';

function _key() {
  var key = PropertiesService.getScriptProperties().getProperty('OVER_CONNECTOR_KEY');
  if (!key) {
    _userError('ה-connector אינו מוגדר (חסר OVER_CONNECTOR_KEY ב-Script Properties).');
  }
  return key;
}

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
    headers: { 'X-Connector-Key': _key() },
    payload: JSON.stringify({ sql: sql, max_rows: maxRows }),
    muteHttpExceptions: true,
  });
  if (response.getResponseCode() !== 200) {
    _userError('שגיאה מהשרת של גרסאות לעם: ' + _errorDetail(response));
  }
  return JSON.parse(response.getContentText());
}

/**
 * Dev-only smoke test — run from the Apps Script editor after setting
 * OVER_CONNECTOR_KEY. Success looks like {columns=[x], rows=[{x=1}], ...};
 * an HTML/403 response means Cloudflare is challenging Google's egress IPs
 * and needs a WAF skip rule for /api/connector/*.
 */
function smoke() {
  Logger.log(apiSql('SELECT 1 AS x', 1));
}

/** The trimmed table catalog for the config dropdown. */
function fetchTables() {
  var response = UrlFetchApp.fetch(BASE + '/api/connector/tables', {
    headers: { 'X-Connector-Key': _key() },
    muteHttpExceptions: true,
  });
  if (response.getResponseCode() !== 200) {
    _userError('לא ניתן לטעון את רשימת הטבלאות: ' + _errorDetail(response));
  }
  return JSON.parse(response.getContentText()).tables || [];
}
