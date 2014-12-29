using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using prefSQL.SQLParser.Models;

namespace prefSQL.SQLParser
{
    
    class SQLSort
    {

        //Create the ORDERBY-Clause from the preferene model
        //public string getSortClause(PrefSQLModel model)
        public string getSortClause(PrefSQLModel model, SQLCommon.Ordering type)
        {
            string strSQL = "";
            switch (type)
            {
                case SQLCommon.Ordering.AttributePosition:
                    strSQL = getSortAttributePositionClause(model);
                    break;
                case SQLCommon.Ordering.RankingSummarize:
                    strSQL = getSortRankingSumClause(model);
                    break;
                case SQLCommon.Ordering.RankingBestOf:
                    strSQL = getSortRankingBestOfClause(model);
                    break;
                case SQLCommon.Ordering.AsIs:
                    strSQL = ""; //Return no ORDER BY Clause
                    break;
                case SQLCommon.Ordering.Random:
                    strSQL = getSortRandomClause(model);
                    break;
            }

            if (strSQL.Length > 0)
            {
                strSQL = " ORDER BY " + strSQL;
            }
            return strSQL;
        }

    
        /**
         *  Sorts the results according to the attributes values. the first attribute has the highest priority.
         *  For example a tuple has the attributes price and color. The result will be sorted after price and color, whereas 
         *  the price has the higher priority
         * 
         * */
        private string getSortAttributePositionClause(PrefSQLModel model)
        {
            string strSQL = "";
            for (int iChild = 0; iChild < model.Skyline.Count; iChild++)
            {
                //First record doesn't need a comma to separate
                if (iChild > 0)
                {
                    strSQL += ", ";
                }
                strSQL += model.Skyline[iChild].OrderBy.ToString();
            }
            return strSQL;
        }
       


        /**
         *  Sorts the results according to their summed ranking. 
         *  For example a tuple has the best, 5th and 7th rank in three attributes. This leads to a ranking of 13.
         * 
         * */
    
    private string getSortRankingSumClause(PrefSQLModel model)
        {
            string strSQL = "";


            for (int iChild = 0; iChild < model.Skyline.Count; iChild++)
            {
                //First attribute doesn't need a plus
                if(iChild > 0)
                {
                    strSQL += " + ";
                }
                strSQL += model.Skyline[iChild].Expression.Replace("ROW_NUMBER()", "DENSE_RANK()");

            }

            return strSQL;
        }
    

        /**
         *  Sorts the results according to their best ranking of all attributes
         *  For example a tuple has the best, 5th and 7th rank in three attributes. This leads to a ranking of 1.
         * 
         * */
        private string getSortRankingBestOfClause(PrefSQLModel model)
        {
            string strSQL = "CASE ";
            string strRanking = "";

            for (int iChild = 0; iChild < model.Skyline.Count; iChild++)
            {
                if (iChild == model.Skyline.Count - 1)
                {
                    //Last record only needs ELSE
                    strSQL += " ELSE " + "" + model.Skyline[iChild].Expression;
                }
                else
                {
                    strSQL += "WHEN ";
                    strRanking = model.Skyline[iChild].Expression;
                    for (int iSubChild = iChild + 1; iSubChild < model.Skyline.Count; iSubChild++)
                    {
                        strSQL += strRanking + " <=" + model.Skyline[iSubChild].Expression;
                        if (iSubChild < model.Skyline.Count - 1)
                        {
                            strSQL += " AND ";
                        }
                    }
                    strSQL += " THEN " + model.Skyline[iChild].Expression + " ";
                }
                
            }
            strSQL += " END";

            return strSQL;
        }
    

        /**
         *  Sorts the results according to their best ranking of all attributes
         *  For example a tuple has the best, 5th and 7th rank in three attributes. This leads to a ranking of 1.
         * 
         * */
        private string getSortRandomClause(PrefSQLModel model)
        {
            string strSQL = "NEWID()";            

            return strSQL;
        }
        
    }
      
}
