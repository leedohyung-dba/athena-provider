// fieldリスト
const REC_COLS = {
  // 'field_1': null,
  // 'field_2': ''
}
// 必須fieldリスト
const REC_REQUIRED_COLS = [
  // 'field_1'
]

// テーブルPK
const TABLE_PAR_KEY = process.env.TABLE_PAR_KEY
// S3 BUCKET
const BUCKET = process.env.BUCKET
// ATHENA Query結果の出力先
const ATHENA_OUTPUT_LOCATION = process.env.ATHENA_OUTPUT_LOCATION
// ATHENA DB名
const ATHENA_DATEBASE = process.env.ATHENA_DATEBASE

// WHEREの'='条件で検索する項目
const SEARCH_EQ_TYPE_COLUMNS = []
// WHEREの'like'条件で検索する項目
const SEARCH_LIKE_TYPE_COLUMNS = []
// WHEREの'date()='条件で検索する項目
const SEARCH_DATE_TYPE_COLUMNS = [];
// WHEREの'date()='条件で検索する項目
const ATHENA_TABLE_OF_PROJECTS = {
  // systemcode1: process.env.ATHENA_TABLE_OF_SYSTEMCODE1
  // systemcode2: process.env.ATHENA_TABLE_OF_SYSTEMCODE2
}
const S3_OBJECT_PREFIXS_OF_PROJECTS = {
  // systemcode1_system: process.env.S3_OBJECT_PREFIXS_OF_SYSTEMCODE1
  // systemcode2_system: process.env.S3_OBJECT_PREFIXS_OF_SYSTEMCODE2
}

module.exports.REC_COLS = REC_COLS
module.exports.REC_REQUIRED_COLS = REC_REQUIRED_COLS

module.exports.TABLE_PAR_KEY = TABLE_PAR_KEY
module.exports.BUCKET = BUCKET
module.exports.S3_OBJECT_PREFIXS_OF_PROJECTS = S3_OBJECT_PREFIXS_OF_PROJECTS
module.exports.ATHENA_OUTPUT_LOCATION = ATHENA_OUTPUT_LOCATION
module.exports.ATHENA_DATEBASE = ATHENA_DATEBASE

module.exports.SEARCH_EQ_TYPE_COLUMNS = SEARCH_EQ_TYPE_COLUMNS
module.exports.SEARCH_LIKE_TYPE_COLUMNS = SEARCH_LIKE_TYPE_COLUMNS
module.exports.SEARCH_DATE_TYPE_COLUMNS = SEARCH_DATE_TYPE_COLUMNS
module.exports.ATHENA_TABLE_OF_PROJECTS = ATHENA_TABLE_OF_PROJECTS
