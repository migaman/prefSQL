using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace prefSQL.SQLParser.Models
{
    class RankModel
    {
        public RankModel(string strExpression, string strTableName, string strColumnName)
        {
            expression = strExpression;
            TableName = strTableName;
            ColumnName = strColumnName;
        }

        public string ColumnName { get; set; }
        public string TableName { get; set; }
        public string expression { get; set; }

    }
}
