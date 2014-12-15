using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace prefSQL.SQLParser.Models
{
    class RankModel
    {
        public RankModel(string strRankColumn, string strTableName, string strColumnName, string strExpression, string strRankHexagon)
        {
            Expression = strExpression;
            TableName = strTableName;
            ColumnName = strColumnName;
            RankColumn = strRankColumn;
            RankHexagon = strRankHexagon;
        }

        public string ColumnName { get; set; }
        public string TableName { get; set; }
        public string Expression { get; set; }
        public string RankColumn { get; set; }

        public string RankHexagon { get; set; }

    }
}
