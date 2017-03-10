using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using prefSQL.SQLParser;
using prefSQL.SQLSkyline;
using System.Data;
using System.Data.SqlClient;

namespace IssueTest
{
    [TestClass]
    public class GitHubIssues
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

            string expectedBNLSort = "EXEC dbo.prefSQL_SkylineBNLLevel 'SELECT  CAST(t1.price AS bigint) AS SkylineAttribute0 , t1.id , CAST(t1.price AS bigint) AS SkylineAttributet1_price FROM cars_small t1 ORDER BY t1.price ASC', 'LOW', 0, 4";

            Assert.AreEqual(parsedSQL, expectedBNLSort, "Query does not match parsed Query");
            
        }

        //Internal attributes naming: add table identifier to the name
        //https://github.com/migaman/prefSQL/issues/48
        [TestMethod]
        public void TestIssue48()
        {
            string prefSQL = "SELECT t1.id, co.name, bo.name FROM cars_small t1 "
                + "LEFT OUTER JOIN colors co ON t1.color_id = co.id "
                + "LEFT OUTER JOIN bodies bo ON t1.body_id = bo.id "
                + "ORDER BY WEIGHTEDSUM (co.Name ('pink' >> 'black' >> OTHERS EQUAL) 0.4 " 
                + ", bo.Name ('compact car' >> 'coupé' >> OTHERS EQUAL) 0.6)";

            SQLCommon common = new SQLCommon();
            common.SkylineType = new SkylineBNLSort();
            common.ShowInternalAttributes = true;

            string parsedSQL = common.ParsePreferenceSQL(prefSQL);

            string expectedBNLSort = "EXEC dbo.prefSQL_Ranking 'SELECT t1.id, co.name, bo.name FROM cars_small t1 LEFT OUTER JOIN colors co ON t1.color_id = co.id LEFT OUTER JOIN bodies bo ON t1.body_id = bo.id ORDER BY WEIGHTEDSUM (co.Name (''pink'' >> ''black'' >> OTHERS EQUAL) 0.4 , bo.Name (''compact car'' >> ''coupé'' >> OTHERS EQUAL) 0.6)', 'SELECT MIN(CASE WHEN co.Name IN (''pink'') THEN 0 WHEN co.Name IN (''black'') THEN 1 ELSE 2 END), MAX(CASE WHEN co.Name IN (''pink'') THEN 0 WHEN co.Name IN (''black'') THEN 1 ELSE 2 END) FROM colors co;SELECT MIN(CASE WHEN bo.Name IN (''compact car'') THEN 0 WHEN bo.Name IN (''coupé'') THEN 1 ELSE 2 END), MAX(CASE WHEN bo.Name IN (''compact car'') THEN 0 WHEN bo.Name IN (''coupé'') THEN 1 ELSE 2 END) FROM bodies bo', 0, '0.4;0.6', 'CASE WHEN co.Name IN (''pink'') THEN 0 WHEN co.Name IN (''black'') THEN 1 ELSE 2 END;CASE WHEN bo.Name IN (''compact car'') THEN 0 WHEN bo.Name IN (''coupé'') THEN 1 ELSE 2 END', True, 'co_Name;bo_Name'";

            Assert.AreEqual(parsedSQL, expectedBNLSort, "Query does not match parsed Query");

        }



        //Concatenation of constant and attribute values result in error
        //https://github.com/migaman/prefSQL/issues/49
        //TODO: Doppelte Anführungszeichen verhindern
        [TestMethod]
        public void TestIssue49()
        {
            string prefSQL = "SELECT c.id as id, c.title as name, 'Constant ' + c.reference as Test FROM cars as c ORDER BY WEIGHTEDSUM(c.price around 1 1.0)";

            SQLCommon common = new SQLCommon();
            common.ShowInternalAttributes = true;

            string parsedSQL = common.ParsePreferenceSQL(prefSQL);

            string expectedBNLSort = "EXEC dbo.prefSQL_Ranking 'SELECT c.id as id, c.title as name, ''Constant '' + c.reference as Test FROM cars as c ORDER BY WEIGHTEDSUM(c.price around 1 1.0)', 'SELECT MIN(ABS(c.price - 1)), MAX(ABS(c.price - 1)) FROM cars c', 0, '1', 'ABS(c.price - 1)', True, 'c_price'";

            Assert.AreEqual(parsedSQL, expectedBNLSort, "Query does not match parsed Query");

        }


        //Concatenation of constant and attribute values result in error
        //https://github.com/migaman/prefSQL/issues/54
        [TestMethod]
        [TestCategory("UnitTest")]
        public void TestIssue54()
        {
            string prefSQL = "SELECT c.Title AS Name, c.Price, c.Consumption, m.Name AS Manufacturer, b.Name AS Body "
                              + "FROM Cars c "
                              + "LEFT OUTER JOIN Makes m ON c.Make_Id = m.Id "
                              + "LEFT OUTER JOIN Bodies b ON c.Body_Id = b.Id "
                              + "WHERE m.Name = 'VW' AND b.Name = 'Bus' "
                              + "SKYLINE OF c.Price LOW 1000 EQUAL, c.Consumption LOW";

            var common = new SQLCommon
            {
                SkylineType = new SkylineBNL(),
                ShowInternalAttributes = true
            };


            try
            {
                //If there is no exception in the execution of this query the test is successful
                DataTable dt = common.ParseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName, prefSQL);
                Assert.IsTrue(true);
            }
            catch
            {
                Assert.IsFalse(true);
            }
        }



        //Parses fails if identifier contain keywords
        //https://github.com/migaman/prefSQL/issues/63
        [TestMethod]
        [TestCategory("UnitTest")]
        public void TestIssue63()
        {
            string prefSQL = "SELECT TOP 5 c.Id, c.title AS IDFROM, c.title AS IDWHERE, c.title AS IDTOP "
                            + "FROM Cars c "
                            + "SKYLINE OF c.id LOW ";

            var common = new SQLCommon
            {
                SkylineType = new SkylineBNL(),
                ShowInternalAttributes = true
            };


            try
            {
                //If there is no exception in the execution of this query the test is successful
                DataTable dt = common.ParseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName, prefSQL);
                Assert.IsTrue(true);
            }
            catch
            {
                Assert.IsFalse(true);
            }
        }


        //Nested queries not supported  
        //https://github.com/migaman/prefSQL/issues/66
        [TestMethod]
        [TestCategory("UnitTest")]
        public void TestIssue66()
        {
            string prefSQL = " SELECT * FROM ( SELECT * FROM cars )b";

            var common = new SQLCommon
            {
                SkylineType = new SkylineBNL(),
                ShowInternalAttributes = false
            };


            try
            {
                //If there is no exception in the execution of this query the test is successful
                DataTable dt = common.ParseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName, prefSQL);
                Assert.IsTrue(true);
            }
            catch
            {
                Assert.IsFalse(true);
            }
        }



        //TemplateStrategy.SkylineValues not set for algorithms other than BNL
        //https://github.com/migaman/prefSQL/issues/67
        [TestMethod]
        [TestCategory("UnitTest")]
        public void TestIssue67()
        {
            string prefSQL = "SELECT c.Title as Name, c.Price, co.Name As Color  "
                            + "FROM Cars c "
                            + "LEFT OUTER JOIN Colors co ON c.Color_Id = co.Id "
                            + "SKYLINE OF c.Price HIGH, co.Name ('pink' >> 'black' >> OTHERS EQUAL)  "
                            + "ORDER BY BEST_RANK()";


            SQLCommon common = new SQLCommon();
            common.SkylineType = new SkylineDQ(); 

            try
            {
                //If there is no exception in the execution of this query the test is successful
                DataTable dtStandalone = common.ParseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName, prefSQL);

                String sql = common.ParsePreferenceSQL(prefSQL);
                SqlConnection cnnSQL = new SqlConnection(Helper.ConnectionString); //for CLR performance tets
                cnnSQL.Open();
                SqlDataAdapter dap = new SqlDataAdapter(sql, cnnSQL);
                DataTable dtCLR = new DataTable();
                dap.Fill(dtCLR);

                Assert.AreEqual(dtStandalone.Rows.Count, dtCLR.Rows.Count);
            }
            catch
            {
                Assert.IsFalse(true);
            }
        }


        

    }
}
