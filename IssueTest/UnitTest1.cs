using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using prefSQL.SQLParser;
using prefSQL.SQLSkyline;

namespace IssueTest
{
    [TestClass]
    public class UnitTest1
    {
        //Observations: In front of the 'ORDER BY' keyword a white space character is needed for strategies:
        //https://github.com/migaman/prefSQL/issues/47
        [TestMethod]
        public void TestIssue47()
        {
            string prefSQL = "SELECT t1.id FROM cars_small t1 SKYLINE OF t1.price LOW ORDER BY t1.price ASC";

            SQLCommon common = new SQLCommon();
            common.SkylineType = new SkylineBNL();
            common.ShowInternalAttributes = true;

            string parsedSQL = common.ParsePreferenceSQL(prefSQL);

            string expectedBNLSort = "EXEC dbo.prefSQL_SkylineBNLLevel 'SELECT CAST(t1.price AS bigint) AS SkylineAttribute0 , t1.id , CAST(t1.price AS bigint) AS SkylineAttributet1_price FROM cars_small t1ORDER BY t1.price ASC ', 'LOW', 0, 5";

            Assert.AreEqual(parsedSQL, expectedBNLSort, "Query does not expected parsed Query");
            
        }

        //Internal attributes naming: add table identifier to the name
        //https://github.com/migaman/prefSQL/issues/48
        [TestMethod]
        public void TestIssue48()
        {
            string prefSQL = "SELECT t1.id, co.name, bo.name FROM cars_small t1 "
                + "LEFT OUTER JOIN colors co ON t1.color_id = co.id "
                + "LEFT OUTER JOIN bodies bo ON t1.body_id = bo.id "
                //+ "ORDER BY WEIGHTEDSUM(t1.price LOW 1)";
                + "ORDER BY WEIGHTEDSUM (co.Name ('pink' >> 'black' >> OTHERS EQUAL) 0.4 " 
                + ", bo.Name ('compact car' >> 'coupé' >> OTHERS EQUAL) 0.6)";

            SQLCommon common = new SQLCommon();
            common.SkylineType = new SkylineBNLSort();
            common.ShowInternalAttributes = true;

            string parsedSQL = common.ParsePreferenceSQL(prefSQL);

            string expectedBNLSort = "EXEC dbo.prefSQL_SkylineBNLLevel 'SELECT CAST(t1.price AS bigint) AS SkylineAttribute0 , t1.id , CAST(t1.price AS bigint) AS SkylineAttributet1_price FROM cars_small t1ORDER BY t1.price ASC ', 'LOW', 0, 5";

            Assert.AreEqual(parsedSQL, expectedBNLSort, "Query does not expected parsed Query");

        }



    }
}
