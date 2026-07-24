/**
 * Looker Studio community connector for OVER (גרסאות לעם, over.org.il).
 *
 * Wraps the key-gated read-only SQL API (/api/connector/sql) so anyone can
 * build dashboards on OVER's data without database credentials. Users pick a
 * table from the live catalog or write free SQL (SELECT/WITH only — the
 * backend enforces read-only, a statement timeout and a 50k row cap).
 *
 * Field IDs are content-derived (md5 of the column name), NOT positional:
 * Looker persists field IDs inside every chart, so positional ids (c0, c1…)
 * would silently rebind charts to the wrong column whenever the user edits
 * their SQL and inserts a column mid-list. Hebrew column names ride along as
 * display labels — the reason this connector exists at all (the native
 * Postgres connector is ASCII-only).
 */

var cc = DataStudioApp.createCommunityConnector();

function getAuthType() {
  return cc.newAuthTypeResponse().setAuthType(cc.AuthType.NONE).build();
}

// Released: false hides stack traces from end users. Flip to true locally
// when debugging in Looker Studio.
function isAdminUser() {
  return false;
}

function getConfig() {
  var config = cc.getConfig();

  config
    .newInfo()
    .setId('intro')
    .setText(
      'בחרו טבלה מהקטלוג של גרסאות לעם, או כתבו שאילתת SQL חופשית ' +
      '(SELECT בלבד; אפשר לחצות בין מאגרי data.gov.il לטבלאות הכנסת). ' +
      'שאילתה חופשית גוברת על הטבלה שנבחרה.'
    );

  var tableSelect = config
    .newSelectSingle()
    .setId('table')
    .setName('טבלה')
    .setHelpText('הקטלוג המלא זמין גם ב-over.org.il/data');
  fetchTables().forEach(function (t) {
    var label = (t.title || t.table) + ' — ' + t.table;
    tableSelect.addOption(
      config.newOptionBuilder().setLabel(label).setValue(t.schema + '.' + t.table)
    );
  });

  config
    .newTextArea()
    .setId('sql')
    .setName('SQL חופשי (אופציונלי, גובר על הטבלה)')
    .setPlaceholder('SELECT ... FROM ... LIMIT 1000');

  config
    .newTextInput()
    .setId('row_limit')
    .setName('מקסימום שורות')
    .setHelpText('ברירת מחדל 10,000, עד 50,000. לטבלאות רחבות מומלץ להקטין או לסכם ב-SQL.');

  config.setDateRangeRequired(false);
  return config.build();
}

function effectiveSql(configParams) {
  var cp = configParams || {};
  if (cp.sql && cp.sql.trim()) {
    return cp.sql.trim();
  }
  if (cp.table) {
    var parts = cp.table.split('.');
    return 'SELECT * FROM "' + parts[0] + '"."' + parts[1] + '"';
  }
  _userError('בחרו טבלה או הזינו שאילתת SQL.');
}

// ── Field definitions ──────────────────────────────────────────────────────

/** CKAN-style backend type → Looker Studio typing + value conversion kind. */
var TYPE_MAP = {
  int: { dataType: 'NUMBER', semanticType: 'NUMBER', metric: true, convert: 'number' },
  numeric: { dataType: 'NUMBER', semanticType: 'NUMBER', metric: true, convert: 'number' },
  bool: { dataType: 'BOOLEAN', semanticType: 'BOOLEAN', metric: false, convert: 'raw' },
  timestamp: { dataType: 'STRING', semanticType: 'YEAR_MONTH_DAY_SECOND', metric: false, convert: 'datetime' },
};
var TYPE_DEFAULT = { dataType: 'STRING', semanticType: 'TEXT', metric: false, convert: 'string' };

function _md5hex(text) {
  var digest = Utilities.computeDigest(
    Utilities.DigestAlgorithm.MD5, text, Utilities.Charset.UTF_8
  );
  return digest
    .map(function (b) {
      return ((b + 256) % 256).toString(16).padStart(2, '0');
    })
    .join('');
}

/**
 * Build internal field defs from the backend envelope's fields/columns.
 * Each def: {id, label, ckan, spec} where spec is the TYPE_MAP entry.
 * Duplicate column names get _1, _2… suffixes by occurrence order.
 */
function buildFieldDefs(envelope) {
  var seen = {};
  return envelope.fields.map(function (f) {
    var id = 'c_' + _md5hex(f.id).slice(0, 10);
    if (seen[id] !== undefined) {
      seen[id] += 1;
      id = id + '_' + seen[id];
    } else {
      seen[id] = 0;
    }
    return { id: id, label: f.id, ckan: f.type, spec: TYPE_MAP[f.type] || TYPE_DEFAULT };
  });
}

/** Internal defs → the schema array Looker Studio expects. */
function toLookerSchema(defs) {
  return defs.map(function (d) {
    var field = {
      name: d.id,
      label: d.label,
      dataType: d.spec.dataType,
      semantics: {
        conceptType: d.spec.metric ? 'METRIC' : 'DIMENSION',
        semanticType: d.spec.semanticType,
      },
    };
    if (d.spec.metric) {
      field.semantics.isReaggregatable = true;
    }
    return field;
  });
}

function getSchema(request) {
  var envelope = apiSql(effectiveSql(request.configParams), 1);
  return { schema: toLookerSchema(buildFieldDefs(envelope)) };
}

// ── Data ───────────────────────────────────────────────────────────────────

function convertValue(value, spec) {
  if (value === null || value === undefined) {
    return null;
  }
  switch (spec.convert) {
    case 'raw':
      return value;
    case 'number': {
      var n = Number(value); // numeric/Decimal arrives as a string
      return isNaN(n) ? null : n;
    }
    case 'datetime': {
      // "2024-01-05 12:33:00+00:00" → "20240105123300"; date-only pads to
      // midnight. Taking the first 14 digits BEFORE padding drops the
      // timezone-offset digits harmlessly.
      var digits = String(value).replace(/\D/g, '').slice(0, 14);
      while (digits.length < 14) {
        digits += '0';
      }
      return digits;
    }
    default:
      return String(value);
  }
}

function getData(request) {
  var sample = request.scriptParams && request.scriptParams.sampleExtraction;
  var maxRows;
  if (sample) {
    maxRows = 100; // Looker only wants a sample (field inference / preview)
  } else {
    var cp = request.configParams || {};
    maxRows = parseInt(cp.row_limit, 10) || 10000;
    maxRows = Math.min(Math.max(1, maxRows), 50000);
  }

  var envelope = apiSql(effectiveSql(request.configParams), maxRows);
  var defs = buildFieldDefs(envelope);
  var byId = {};
  defs.forEach(function (d) {
    byId[d.id] = d;
  });

  // Each chart requests only its own fields, in its own order — the response
  // schema and every values array must match that exact subset and order.
  var requested = request.fields.map(function (f) {
    var d = byId[f.name];
    if (!d) {
      _userError('השדות השתנו מאז שהוגדר מקור הנתונים — פתחו את מקור הנתונים ולחצו "רענון שדות".');
    }
    return d;
  });

  var rows = envelope.rows.map(function (record) {
    return {
      values: requested.map(function (d) {
        return convertValue(record[d.label], d.spec);
      }),
    };
  });

  return { schema: toLookerSchema(requested), rows: rows };
}
