using prefSQL.SQLParser.Models;

namespace prefSQL.SQLParser
{
    //internal class
    class SQLSort
    {

        /// <summary>
        /// Create the ORDER BY-Clause from the preference model 
        /// </summary>
        /// <param name="model"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public string GetSortClause(PrefSQLModel model, SQLCommon.Ordering type)
        {
            string strSQL = "";
            switch (type)
            {
                case SQLCommon.Ordering.AttributePosition:
                    strSQL = GetSortAttributePositionClause(model);
                    break;
                case SQLCommon.Ordering.RankingSummarize:
                    strSQL = GetSortRankingSumClause(model);
                    break;
                case SQLCommon.Ordering.RankingBestOf:
                    strSQL = GetSortRankingBestOfClause(model);
                    break;
                case SQLCommon.Ordering.AsIs:
                    strSQL = ""; //Return no ORDER BY Clause
                    break;
                case SQLCommon.Ordering.Random:
                    strSQL = GetSortRandomClause(model);
                    break;
            }

            if (strSQL.Length > 0)
            {
                strSQL = " ORDER BY " + strSQL;
            }
            return strSQL;
        }

    

        /// <summary>
        ///  Sorts the results according to the attributes values. the first attribute has the highest priority.
        ///  For example a tuple has the attributes price and color. The result will be sorted after price and color, whereas 
        ///  the price has the higher priority
        /// </summary>
        /// <param name="model"></param>
        /// <returns></returns>
        private string GetSortAttributePositionClause(PrefSQLModel model)
        {
            string strSQL = "";
            for (int iChild = 0; iChild < model.Skyline.Count; iChild++)
            {
                //First record doesn't need a comma to separate
                if (iChild > 0)
                {
                    strSQL += ", ";
                }
                //strSQL += model.Skyline[iChild].OrderBy.ToString();

                strSQL += model.Skyline[iChild].Expression;
            }
            return strSQL;
        }


        /// <summary>
        /// Sorts the results according to their summed ranking. 
        /// For example a tuple has the best, 5th and 7th rank in three attributes. This leads to a ranking of 13.
        /// </summary>
        /// <param name="model"></param>
        /// <returns></returns>
        private string GetSortRankingSumClause(PrefSQLModel model)
        {
            string strSQL = "";


            for (int iChild = 0; iChild < model.Skyline.Count; iChild++)
            {
                //First attribute doesn't need a plus
                if(iChild > 0)
                {
                    strSQL += " + ";
                }
                string strRankingExpression = "DENSE_RANK() OVER (ORDER BY " + model.Skyline[iChild].Expression + ")";
                strSQL += strRankingExpression;

            }

            return strSQL;
        }


        /// <summary>
        /// Sorts the results according to their best ranking of all attributes
        /// For example a tuple has the best, 5th and 7th rank in three attributes. This leads to a ranking of 1.
        /// </summary>
        /// <param name="model"></param>
        /// <returns></returns>
        private string GetSortRankingBestOfClause(PrefSQLModel model)
        {
            string strSQL = "CASE ";

            for (int iChild = 0; iChild < model.Skyline.Count; iChild++)
            {
                string strRankingExpression = "DENSE_RANK() OVER (ORDER BY " + model.Skyline[iChild].Expression + ")";
                strRankingExpression = strRankingExpression.Replace("DENSE_RANK()", "ROW_NUMBER()");

                if (model.Skyline.Count == 1)
                {
                    //special case if totally only one preference
                    strSQL += "WHEN 1=1 THEN " + strRankingExpression + " ";
                }
                if (iChild == model.Skyline.Count - 1)
                {
                    //Last record only needs ELSE
                    strSQL += " ELSE " + "" + strRankingExpression;
                }
                else
                {
                    strSQL += "WHEN ";
                    var strRanking = strRankingExpression;
                    for (int iSubChild = iChild + 1; iSubChild < model.Skyline.Count; iSubChild++)
                    {
                        string strSubRanking = "DENSE_RANK() OVER (ORDER BY " + model.Skyline[iSubChild].Expression + ")";
                        strSubRanking = strSubRanking.Replace("DENSE_RANK()", "ROW_NUMBER()");
                        strSQL += strRanking + " <=" + strSubRanking;
                        if (iSubChild < model.Skyline.Count - 1)
                        {
                            strSQL += " AND ";
                        }
                    }
                    strSQL += " THEN " + strRankingExpression + " ";
                }
                
            }
            strSQL += " END";

            return strSQL;
        }

    
        /// <summary>
        /// Sorts the results according to their best ranking of all attributes
        /// For example a tuple has the best, 5th and 7th rank in three attributes. This leads to a ranking of 1.
        /// </summary>
        /// <param name="model"></param>
        /// <returns></returns>
        private string GetSortRandomClause(PrefSQLModel model)
        {
            string strSQL = "NEWID()";            

            return strSQL;
        }
        
    }
      
}
