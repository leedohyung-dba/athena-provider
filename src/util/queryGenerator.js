'use strict';

const {
  BUCKET,
  TABLE_PAR_KEY,
  S3_OBJECT_PREFIXS,
  ATHENA_ARTISAN_JIKKO_RIREKI_TABLE,
  SEARCH_EQ_TYPE_COLUMNS,
  SEARCH_LIKE_TYPE_COLUMNS,
  SEARCH_DATE_TYPE_COLUMNS
} = require('../../config/table_info.js');

const DATE_CONDITION_OPE = {
  'start': '>=',
  'end': '<='
}

module.exports.queryGenerator = {
  createPartitioningSql: function(part_value) {
    return `
      ALTER TABLE ${ATHENA_ARTISAN_JIKKO_RIREKI_TABLE}
      ADD IF NOT EXISTS PARTITION (${TABLE_PAR_KEY}=\"${part_value}\")
      LOCATION \"s3://${BUCKET}/${S3_OBJECT_PREFIXS}/${TABLE_PAR_KEY}=${part_value}/\"
    `;
  },

  createSelectQuery: function(params, justCountFlg = false) {
    const conditions_str = this._createConditionString(params.conditions)
    if (!justCountFlg) return this._createSelectQueryString(conditions_str, params.count, params.page);
    else return this._createCountSelectQueryString(conditions_str);
  },

  _createConditionString: function(conditions) {
    var conditions_arr = [];
    for (let [col, val] of Object.entries(conditions)) {
      if (SEARCH_EQ_TYPE_COLUMNS.includes(col) && val != null) conditions_arr.push(`${col} = '${val}'`);
      else if (SEARCH_LIKE_TYPE_COLUMNS.includes(col) && val != null) conditions_arr.push(`${col} like '%${val}%'`);
      else if (SEARCH_DATE_TYPE_COLUMNS.includes(col)) {
        Object.keys(val).forEach((key) => {
          if (val[key]) conditions_arr.push(`${col} ${DATE_CONDITION_OPE[key]} date('${val[key]}')`);
        })
      }
    }
    return conditions_arr.join(' AND ')
  },

  _createSelectQueryString: function(conditions_str, count = 10, page = 1) {
    return `
      SELECT * FROM (
      SELECT row_number() over(ORDER BY created DESC) AS rn, *
      FROM ${ATHENA_ARTISAN_JIKKO_RIREKI_TABLE} ${(conditions_str) ? 'WHERE ' + conditions_str : ''}
      ) WHERE rn BETWEEN ${((page-1) * count)+1} AND ${((page-1) * count)+count};
    `;
  },

  _createCountSelectQueryString: function(conditions_str) {
    return `
      SELECT count(*) AS total
      FROM ${ATHENA_ARTISAN_JIKKO_RIREKI_TABLE}
      ${(conditions_str) ? 'WHERE ' + conditions_str : ''};
    `;
  }
}


