using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using prefSQL.SQLParser;
using prefSQL.SQLParser.Models;
using prefSQL.SQLSkyline;

namespace prefSQL.SQLParserTest
{
    [TestClass]
    public class SQLParserSkylineTest
    {
        public TestContext TestContext { get; set; }
        /**
         * This test checks if the algorithms return the same amount of tupels for different prefSQL statements
         * At the moment it contains the BNL, nativeSQL and Hexagon algorithm. It is intendend to add the D&Q as soon
         * as it works
         * 
         * */
        [TestMethod]
        [DataSource("Microsoft.VisualStudio.TestTools.DataSource.XML", "SQLParserSkylineTest.xml", "TestDataRow",
            DataAccessMethod.Sequential),
         DeploymentItem("SQLParserSkylineTest.xml")]
        public void TestSkylineAmountOfTupelsMSSQLCLR()
        {
            string skylineSampleSql = TestContext.DataRow["skylineSQL"].ToString();

            SQLCommon common = new SQLCommon();
            common.SkylineType = new SkylineSQL();
            PrefSQLModel model = common.GetPrefSqlModelFromPreferenceSql(skylineSampleSql);
            string sqlNative = common.GetAnsiSqlFromPrefSqlModel(model);
            common.SkylineType = new SkylineBNL();
            string sqlBNL = common.ParsePreferenceSQL(skylineSampleSql);
            common.SkylineType = new SkylineBNLSort();
            string sqlBNLSort = common.ParsePreferenceSQL(skylineSampleSql);
            common.SkylineType = new SkylineHexagon();
            string sqlHexagon = common.ParsePreferenceSQL(skylineSampleSql);
            //D&Q does not run with CLR
            common.SkylineType = new SkylineDQ();
            string sqlDQ = common.ParsePreferenceSQL(skylineSampleSql);

            int amountOfTupelsBNL = 0;
            int amountOfTupelsBNLSort = 0;
            int amountOfTupelsSQL = 0;
            int amountOfTupelsHexagon = 0;
            int amountOfTupelsDQ = 0;

            SqlConnection cnnSQL = new SqlConnection(Helper.ConnectionString);
            cnnSQL.InfoMessage += cnnSQL_InfoMessage;
            try
            {
                cnnSQL.Open();

                //Native
                DbCommand command = cnnSQL.CreateCommand();
                command.CommandTimeout = 0; //infinite timeout
                command.CommandText = sqlNative;
                DbDataReader sqlReader = command.ExecuteReader();

                if (sqlReader.HasRows)
                {
                    while (sqlReader.Read())
                    {
                        amountOfTupelsSQL++;
                    }
                }
                sqlReader.Close();

                //BNL
                command.CommandText = sqlBNL;
                sqlReader = command.ExecuteReader();

                if (sqlReader.HasRows)
                {
                    while (sqlReader.Read())
                    {
                        amountOfTupelsBNL++;
                    }
                }
                sqlReader.Close();

                //BNLSort
                command.CommandText = sqlBNLSort;
                sqlReader = command.ExecuteReader();

                if (sqlReader.HasRows)
                {
                    while (sqlReader.Read())
                    {
                        amountOfTupelsBNLSort++;
                    }
                }
                sqlReader.Close();


                //Hexagon
                command.CommandText = sqlHexagon;
                sqlReader = command.ExecuteReader();

                if (sqlReader.HasRows)
                {
                    while (sqlReader.Read())
                    {
                        amountOfTupelsHexagon++;
                    }
                }
                sqlReader.Close();

                //D&Q (does not work with incomparable tuples)
                if(model.WithIncomparable == false)
                {
                    command.CommandText = sqlDQ;
                    sqlReader = command.ExecuteReader();

                    if (sqlReader.HasRows)
                    {
                        while (sqlReader.Read())
                        {
                            amountOfTupelsDQ++;
                        }
                    }
                    sqlReader.Close();
                }

                cnnSQL.Close();
            }
            catch (Exception ex)
            {
                Assert.Fail("Connection failed:" + ex.Message);
            }

            int currentDataRowIndex = TestContext.DataRow.Table.Rows.IndexOf(TestContext.DataRow);

            //Check tuples (every algorithm should deliver the same amount of tuples)
            Assert.AreEqual(amountOfTupelsSQL, amountOfTupelsBNLSort, 0,
                "BNLSort Amount of tupels in query " + currentDataRowIndex + " do not match");
            Assert.AreEqual(amountOfTupelsSQL, amountOfTupelsBNL, 0,
                "BNL Amount of tupels in query " + currentDataRowIndex + " do not match");

            //Hexagon cannot handle Categorical preference that have no explicit OTHERS
            if (model.ContainsOpenPreference == false)
            {
                Assert.AreEqual(amountOfTupelsSQL, amountOfTupelsHexagon, 0,
                    "Hexagon Amount of tupels in query " + currentDataRowIndex + " do not match");
            }

            //D&Q does not work with incomparable tuples
            if (model.WithIncomparable == false)
            {
                Assert.AreEqual(amountOfTupelsSQL, amountOfTupelsDQ, 0,
                    "Amount of tupels in query " + currentDataRowIndex + " do not match");
            }
                         
        }

        [TestMethod]
        [DataSource("Microsoft.VisualStudio.TestTools.DataSource.XML", "SQLParserSkylineTest.xml", "TestDataRow",
            DataAccessMethod.Sequential),
         DeploymentItem("SQLParserSkylineTest.xml")]
        public void TestParserSkylineQueries()
        {
            string skylineSampleSql = TestContext.DataRow["skylineSQL"].ToString();

            Dictionary<string, SkylineStrategy> allResultTypes = new Dictionary<string, SkylineStrategy>
            {
                {"parsePreferenceSQLSkylineSQLExpectedResult", new SkylineSQL()},
                {"parsePreferenceSQLSkylineBNLExpectedResult", new SkylineBNL()},
                {"parsePreferenceSQLSkylineBNLSortExpectedResult", new SkylineBNLSort()},
                {"parsePreferenceSQLSkylineHexagonExpectedResult", new SkylineHexagon()},
                {"parsePreferenceSQLSkylineDQExpectedResult", new SkylineDQ()},
                {"parsePreferenceSQLMultipleSkylineBNLExpectedResult", new MultipleSkylineBNL()}
            };

            int currentDataRowIndex = TestContext.DataRow.Table.Rows.IndexOf(TestContext.DataRow);

            SQLCommon common = new SQLCommon();
            foreach (KeyValuePair<string, SkylineStrategy> resultType in allResultTypes)
            {
                common.SkylineType = resultType.Value;
                string parsedSql = common.ParsePreferenceSQL(skylineSampleSql);
                Assert.AreEqual(TestContext.DataRow[resultType.Key].ToString().Trim(), parsedSql.Trim(),
                    "Parsed result in data row " + currentDataRowIndex + " for " + resultType.Key + " is incorrect.");
            }
        }

        /**
         * This test checks if the algorithms return the same amount of tupels for different prefSQL statements
         * At the moment it contains the BNL, nativeSQL and Hexagon algorithm. It is intendend to add the D&Q as soon
         * as it works
         * 
         * */
        [TestMethod]
        [DataSource("Microsoft.VisualStudio.TestTools.DataSource.XML", "SQLParserSkylineTest.xml", "TestDataRow",
            DataAccessMethod.Sequential),
         DeploymentItem("SQLParserSkylineTest.xml")]
        public void TestSkylineAmountOfTupelsDataTable()
        {
            string skylineSampleSql = TestContext.DataRow["skylineSQL"].ToString();

            SQLCommon common = new SQLCommon();
            common.SkylineType = new SkylineSQL();
            PrefSQLModel model = common.GetPrefSqlModelFromPreferenceSql(skylineSampleSql);
            DataTable dtNative = common.ExecuteFromPrefSqlModel(Helper.ConnectionString, Helper.ProviderName, model);
            common.SkylineType = new SkylineBNL();
            DataTable dtBNL = common.ParseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName, skylineSampleSql);
            common.SkylineType = new SkylineBNLSort();
            DataTable dtBNLSort = common.ParseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName, skylineSampleSql);

            DataTable dtHexagon = new DataTable();
            if (model.ContainsOpenPreference == false)
            {
                common.SkylineType = new SkylineHexagon();
                dtHexagon = common.ParseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName, skylineSampleSql);
            }                
                
            DataTable dtDQ = new DataTable();
            //D&Q does not work with incomparable tuples
            if (model.WithIncomparable == false)
            {
                common.SkylineType = new SkylineDQ();
                dtDQ = common.ParseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName, skylineSampleSql);
            }

            int currentDataRowIndex = TestContext.DataRow.Table.Rows.IndexOf(TestContext.DataRow);

            //Check tuples (every algorithm should deliver the same amount of tuples)
            Assert.AreEqual(dtNative.Rows.Count, dtBNL.Rows.Count, 0,
                "BNL Amount of tupels in query " + currentDataRowIndex + " do not match");
            Assert.AreEqual(dtNative.Rows.Count, dtBNLSort.Rows.Count, 0,
                "BNLSort Amount of tupels in query " + currentDataRowIndex + " do not match");

            //Hexagon cannot handle Categorical preference that have no explicit OTHERS
            if (model.ContainsOpenPreference == false)
            {
                Assert.AreEqual(dtNative.Rows.Count, dtHexagon.Rows.Count, 0,
                    "Hexagon Amount of tupels in query " + currentDataRowIndex + " do not match");
            }
            //D&Q does not work with incomparable tuples
            if (model.WithIncomparable == false)
            {
                Assert.AreEqual(dtNative.Rows.Count, dtDQ.Rows.Count, 0,
                    "D&Q Amount of tupels in query " + currentDataRowIndex + " do not match");
            }
                
        }



        [TestMethod]
        public void TestSkylineAmountOfTupelsMultipleLevelsMSSQLCLR()

        {
            string strSQL = "SELECT t1.id, t1.price, t1.mileage FROM cars_small t1 ";
            string strPreferences = " SKYLINE OF t1.price LOW, t1.mileage LOW";
            SQLCommon common = new SQLCommon();

            SqlConnection cnnSQL = new SqlConnection(Helper.ConnectionString);
            cnnSQL.InfoMessage += cnnSQL_InfoMessage;
            try
            {
                cnnSQL.Open();


                //Tree Algorithm
                common.SkylineType = new MultipleSkylineBNL();
                common.SkylineUpToLevel = 3;
                string sqlTree = common.ParsePreferenceSQL(strSQL + strPreferences);
                ArrayList levelRecordsTree = new ArrayList();
                
                DbCommand command = cnnSQL.CreateCommand();
                command.CommandTimeout = 0; //infinite timeout
                command.CommandText = sqlTree;
                DbDataReader dataReader = command.ExecuteReader();

                if (dataReader.HasRows)
                {
                    while (dataReader.Read())
                    {
                        int level = (int)dataReader["level"];
                        if (levelRecordsTree.Count > level)
                        {
                            levelRecordsTree[level] = (int)levelRecordsTree[level] + 1;
                        }
                        else
                        {
                            levelRecordsTree.Add(1);

                        }
                    }
                }
                dataReader.Close();
                

                //BNL Algorithm (multiple times)
                //As long as Query returns skyline tuples
                common.SkylineType = new SkylineBNLSort();
                List<int> listIDs = new List<int>();
                bool isSkylineEmpty = false;
                ArrayList levelRecordsBNLSort = new ArrayList();
                int iLevel = 0;
                while (isSkylineEmpty == false && iLevel < common.SkylineUpToLevel)
                {
                    //Add WHERE clause with IDs that were already in the skyline
                    String strIDs = "";
                    foreach (int id in listIDs)
                    {
                        strIDs += id + ",";
                    }
                    if (strIDs.Length > 0)
                    {
                        strIDs = "WHERE t1.id NOT IN (" + strIDs.TrimEnd(',') + ")";
                    }
                    //Parse PreferenceSQL into SQL
                    string sqlBNLSort = common.ParsePreferenceSQL(strSQL + strIDs + strPreferences);

                    command = cnnSQL.CreateCommand();
                    command.CommandTimeout = 0; //infinite timeout
                    command.CommandText = sqlBNLSort;
                    dataReader = command.ExecuteReader();


                    if (dataReader.HasRows)
                    {
                        while (dataReader.Read())
                        {
                            listIDs.Add(Int32.Parse(dataReader["id"].ToString()));

                            //int level = (int)sqlReader["level"];
                            if (levelRecordsBNLSort.Count > iLevel)
                            {
                                levelRecordsBNLSort[iLevel] = (int)levelRecordsBNLSort[iLevel] + 1;
                            }
                            else
                            {
                                levelRecordsBNLSort.Add(1);

                            }
                        }
                        dataReader.Close();
                    }
                    else
                    {
                        isSkylineEmpty = true;
                    }
                    //Next level
                    iLevel++;
                }


                cnnSQL.Close();


                //Compare the two arrays
                if(levelRecordsTree.Count == levelRecordsBNLSort.Count)
                {
                    for (int i = 0; i < levelRecordsTree.Count; i++ )
                    {
                        if ((int)levelRecordsBNLSort[i] != (int)levelRecordsTree[i])
                        {
                            Assert.Fail("Level " + i + " has another amount of records");
                        }
                    }
                }
                else
                {
                    Assert.Fail("Arrays don't have the same dimension");
                }

            }
            catch (Exception ex)
            {
                Assert.Fail("Connection failed:" + ex.Message);
            }

        }


        [TestMethod]
        public void TestParserSkylineShowInternalAttributes()
        {
            string strPrefSQL = "SELECT * FROM cars SKYLINE OF cars.price LOW, cars.mileage LOW, cars.horsepower HIGH";

            string expected = "SELECT * , CAST(cars.price AS bigint) AS SkylineAttributecars_price, CAST(cars.mileage AS bigint) AS SkylineAttributecars_mileage, CAST(cars.horsepower * -1 AS bigint) AS SkylineAttributecars_horsepower FROM cars WHERE NOT EXISTS(SELECT * , CAST(cars_INNER.price AS bigint) AS SkylineAttributecars_price, CAST(cars_INNER.mileage AS bigint) AS SkylineAttributecars_mileage, CAST(cars_INNER.horsepower * -1 AS bigint) AS SkylineAttributecars_horsepower FROM cars cars_INNER WHERE cars_INNER.price <= cars.price AND cars_INNER.mileage <= cars.mileage AND cars_INNER.horsepower * -1 <= cars.horsepower * -1 AND ( cars_INNER.price < cars.price OR cars_INNER.mileage < cars.mileage OR cars_INNER.horsepower * -1 < cars.horsepower * -1) ) ";
            SQLCommon common = new SQLCommon();
            common.ShowInternalAttributes = true;
            string actual = common.ParsePreferenceSQL(strPrefSQL);

            Assert.AreEqual(expected.Trim(), actual.Trim(), true, "SQL not built correctly");
        }



        [TestMethod]
        public void TestParserSkylineLowWithLevel()
        {
            string strPrefSQL = "SELECT cars.id, cars.title, cars.Price FROM cars SKYLINE OF cars.price LOW 1000 EQUAL, cars.mileage LOW";

            string expected = "SELECT cars.id, cars.title, cars.Price FROM cars WHERE NOT EXISTS(SELECT cars_INNER.id, cars_INNER.title, cars_INNER.Price FROM cars cars_INNER WHERE cars_INNER.price / 1000 <= cars.price / 1000 AND cars_INNER.mileage <= cars.mileage AND ( cars_INNER.price / 1000 < cars.price / 1000 OR cars_INNER.mileage < cars.mileage) ) ";
            SQLCommon common = new SQLCommon();
            string actual = common.ParsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected.Trim(), actual.Trim(), true, "SQL not built correctly");
        }


        [TestMethod]
        public void TestParserSkylineFavourRot()
        {
            string strPrefSQL = "SELECT cars.id, cars.title, cars.Price, colors.Name FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID SKYLINE OF cars.price LOW, colors.name FAVOUR 'red'";

            string expected = "SELECT cars.id, cars.title, cars.Price, colors.Name FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID WHERE NOT EXISTS(SELECT cars_INNER.id, cars_INNER.title, cars_INNER.Price, colors_INNER.Name FROM cars cars_INNER LEFT OUTER JOIN colors colors_INNER ON cars_INNER.color_id = colors_INNER.ID WHERE cars_INNER.price <= cars.price AND CASE WHEN colors_INNER.name = 'red' THEN 1 ELSE 2 END <= CASE WHEN colors.name = 'red' THEN 1 ELSE 2 END AND ( cars_INNER.price < cars.price OR CASE WHEN colors_INNER.name = 'red' THEN 1 ELSE 2 END < CASE WHEN colors.name = 'red' THEN 1 ELSE 2 END) )";
            SQLCommon common = new SQLCommon();
            string actual = common.ParsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected.Trim(), actual.Trim(), true, "SQL not built correctly");
        }



        [TestMethod]
        public void TestParserSkylineWithUnconventialJoin()
        {
            string strPrefSQL = "SELECT cars.id, cars.title, cars.price, colors.name FROM cars, colors WHERE cars.Color_Id = colors.Id SKYLINE OF cars.price LOW, colors.name ('gray' >> 'red')";

            string expected = "SELECT cars.id, cars.title, cars.price, colors.name FROM cars, colors WHERE cars.Color_Id = colors.Id AND NOT EXISTS(SELECT cars_INNER.id, cars_INNER.title, cars_INNER.price, colors_INNER.name FROM cars cars_INNER, colors colors_INNER WHERE cars_INNER.Color_Id = colors_INNER.Id  AND cars_INNER.price <= cars.price AND (CASE WHEN colors_INNER.name = 'gray' THEN 0 WHEN colors_INNER.name = 'red' THEN 100 END <= CASE WHEN colors.name = 'gray' THEN 0 WHEN colors.name = 'red' THEN 100 END OR colors_INNER.name = colors.name) AND ( cars_INNER.price < cars.price OR CASE WHEN colors_INNER.name = 'gray' THEN 0 WHEN colors_INNER.name = 'red' THEN 100 END < CASE WHEN colors.name = 'gray' THEN 0 WHEN colors.name = 'red' THEN 100 END) )";
            SQLCommon common = new SQLCommon();
            string actual = common.ParsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected.Trim(), actual.Trim(), true, "SQL not built correctly");
        }


        [TestMethod]
        public void TestParserSkylineWithWhereClause()
        {
            string strPrefSQL = "SELECT cars.id, cars.title, cars.price, cars.mileage FROM cars WHERE cars.price > 10000 SKYLINE OF cars.price LOW, cars.mileage low";

            string expected = "SELECT cars.id, cars.title, cars.price, cars.mileage FROM cars WHERE cars.price > 10000 AND NOT EXISTS(SELECT cars_INNER.id, cars_INNER.title, cars_INNER.price, cars_INNER.mileage FROM cars cars_INNER WHERE cars_INNER.price > 10000  AND cars_INNER.price <= cars.price AND cars_INNER.mileage <= cars.mileage AND ( cars_INNER.price < cars.price OR cars_INNER.mileage < cars.mileage) )";
            SQLCommon common = new SQLCommon();
            string actual = common.ParsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected.Trim(), actual.Trim(), true, "SQL not built correctly");
        }




        [TestMethod]
        public void TestParserSkylinetopKeyword()
        {
            string strPrefSQL = "SELECT TOP 5 cars.id, cars.title, cars.Price, colors.Name FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID SKYLINE OF cars.price LOW, colors.name ('pink' >> {'red', 'black'} >> 'beige' == 'yellow')";

            string expected = "SELECT TOP 5 cars.id, cars.title, cars.Price, colors.Name FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID WHERE NOT EXISTS(SELECT cars_INNER.id, cars_INNER.title, cars_INNER.Price, colors_INNER.Name FROM cars cars_INNER LEFT OUTER JOIN colors colors_INNER ON cars_INNER.color_id = colors_INNER.ID WHERE cars_INNER.price <= cars.price AND (CASE WHEN colors_INNER.name = 'pink' THEN 0 WHEN colors_INNER.name IN ('red','black') THEN 101 WHEN colors_INNER.name = 'beige' THEN 200 WHEN colors_INNER.name = 'yellow' THEN 200 END <= CASE WHEN colors.name = 'pink' THEN 0 WHEN colors.name IN ('red','black') THEN 100 WHEN colors.name = 'beige' THEN 200 WHEN colors.name = 'yellow' THEN 200 END OR colors_INNER.name = colors.name) AND ( cars_INNER.price < cars.price OR CASE WHEN colors_INNER.name = 'pink' THEN 0 WHEN colors_INNER.name IN ('red','black') THEN 101 WHEN colors_INNER.name = 'beige' THEN 200 WHEN colors_INNER.name = 'yellow' THEN 200 END < CASE WHEN colors.name = 'pink' THEN 0 WHEN colors.name IN ('red','black') THEN 100 WHEN colors.name = 'beige' THEN 200 WHEN colors.name = 'yellow' THEN 200 END) )";
            SQLCommon common = new SQLCommon();
            string actual = common.ParsePreferenceSQL(strPrefSQL);

            // assert
            Assert.AreEqual(expected.Trim(), actual.Trim(), true, "SQL not built correctly");
        }


        [TestMethod]
        public void TestParserSkyline2DimensionsJoinMultipleAccumulation()
        {
            string strPrefSQL = "SELECT cars.id, cars.title, cars.Price, colors.Name FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID SKYLINE OF cars.price LOW, colors.name ('pink' >> {'red', 'black'} >> 'beige' == 'yellow') ORDER BY cars.price ASC, colors.name('pink'>>{'red','black'}>>'beige'=='yellow')";

            string expected = "SELECT cars.id, cars.title, cars.Price, colors.Name FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID WHERE NOT EXISTS(SELECT cars_INNER.id, cars_INNER.title, cars_INNER.Price, colors_INNER.Name FROM cars cars_INNER LEFT OUTER JOIN colors colors_INNER ON cars_INNER.color_id = colors_INNER.ID WHERE cars_INNER.price <= cars.price AND (CASE WHEN colors_INNER.name = 'pink' THEN 0 WHEN colors_INNER.name IN ('red','black') THEN 101 WHEN colors_INNER.name = 'beige' THEN 200 WHEN colors_INNER.name = 'yellow' THEN 200 END <= CASE WHEN colors.name = 'pink' THEN 0 WHEN colors.name IN ('red','black') THEN 100 WHEN colors.name = 'beige' THEN 200 WHEN colors.name = 'yellow' THEN 200 END OR colors_INNER.name = colors.name) AND ( cars_INNER.price < cars.price OR CASE WHEN colors_INNER.name = 'pink' THEN 0 WHEN colors_INNER.name IN ('red','black') THEN 101 WHEN colors_INNER.name = 'beige' THEN 200 WHEN colors_INNER.name = 'yellow' THEN 200 END < CASE WHEN colors.name = 'pink' THEN 0 WHEN colors.name IN ('red','black') THEN 100 WHEN colors.name = 'beige' THEN 200 WHEN colors.name = 'yellow' THEN 200 END) ) ORDER BY cars.price ASC, CASE WHEN colors.name = 'pink' THEN 0 WHEN colors.name IN ('red','black') THEN 100 WHEN colors.name = 'beige' THEN 200 WHEN colors.name = 'yellow' THEN 200 END ASC";
            SQLCommon common = new SQLCommon();
            string actual = common.ParsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected.Trim(), actual.Trim(), true, "SQL not built correctly");
        }







        [TestMethod]
        public void TestParserSkyline2DimensionsJoinOthersAccumulation()
        {
            string strPrefSQL = "SELECT cars.id, cars.title, cars.Price, colors.Name FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID SKYLINE OF cars.price LOW, colors.name ('türkis' >> 'yellow' >> OTHERS INCOMPARABLE)";

            string expected = "SELECT cars.id, cars.title, cars.Price, colors.Name FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID WHERE NOT EXISTS(SELECT cars_INNER.id, cars_INNER.title, cars_INNER.Price, colors_INNER.Name FROM cars cars_INNER LEFT OUTER JOIN colors colors_INNER ON cars_INNER.color_id = colors_INNER.ID WHERE cars_INNER.price <= cars.price AND (CASE WHEN colors_INNER.name = 'türkis' THEN 0 WHEN colors_INNER.name = 'yellow' THEN 100 ELSE 201 END <= CASE WHEN colors.name = 'türkis' THEN 0 WHEN colors.name = 'yellow' THEN 100 ELSE 200 END OR colors_INNER.name = colors.name) AND ( cars_INNER.price < cars.price OR CASE WHEN colors_INNER.name = 'türkis' THEN 0 WHEN colors_INNER.name = 'yellow' THEN 100 ELSE 201 END < CASE WHEN colors.name = 'türkis' THEN 0 WHEN colors.name = 'yellow' THEN 100 ELSE 200 END) )";
            SQLCommon common = new SQLCommon();
            string actual = common.ParsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected.Trim(), actual.Trim(), true, "SQL not built correctly");
        }


        [TestMethod]
        public void TestParserSkyline2DimensionsJoinNoOthers()
        {
            string strPrefSQL = "SELECT cars.id, cars.title, cars.Price, colors.Name FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID SKYLINE OF cars.price LOW, colors.name ('pink' >> 'red' == 'black' >> 'beige' == 'yellow')";

            string expected = "SELECT cars.id, cars.title, cars.Price, colors.Name FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID WHERE NOT EXISTS(SELECT cars_INNER.id, cars_INNER.title, cars_INNER.Price, colors_INNER.Name FROM cars cars_INNER LEFT OUTER JOIN colors colors_INNER ON cars_INNER.color_id = colors_INNER.ID WHERE cars_INNER.price <= cars.price AND (CASE WHEN colors_INNER.name = 'pink' THEN 0 WHEN colors_INNER.name = 'red' THEN 100 WHEN colors_INNER.name = 'black' THEN 100 WHEN colors_INNER.name = 'beige' THEN 200 WHEN colors_INNER.name = 'yellow' THEN 200 END <= CASE WHEN colors.name = 'pink' THEN 0 WHEN colors.name = 'red' THEN 100 WHEN colors.name = 'black' THEN 100 WHEN colors.name = 'beige' THEN 200 WHEN colors.name = 'yellow' THEN 200 END OR colors_INNER.name = colors.name) AND ( cars_INNER.price < cars.price OR CASE WHEN colors_INNER.name = 'pink' THEN 0 WHEN colors_INNER.name = 'red' THEN 100 WHEN colors_INNER.name = 'black' THEN 100 WHEN colors_INNER.name = 'beige' THEN 200 WHEN colors_INNER.name = 'yellow' THEN 200 END < CASE WHEN colors.name = 'pink' THEN 0 WHEN colors.name = 'red' THEN 100 WHEN colors.name = 'black' THEN 100 WHEN colors.name = 'beige' THEN 200 WHEN colors.name = 'yellow' THEN 200 END) )";
            SQLCommon common = new SQLCommon();
            string actual = common.ParsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected.Trim(), actual.Trim(), true, "SQL not built correctly");
        }



        [TestMethod]
        public void TestParserSkyline2DimensionsNoJoin()
        {
            string strPrefSQL = "SELECT cars.id, cars.price, cars.title FROM cars SKYLINE OF cars.title ('MERCEDES-BENZ SL 600' >> OTHERS EQUAL), cars.price LOW";

            string expected = "SELECT cars.id, cars.price, cars.title FROM cars WHERE NOT EXISTS(SELECT cars_INNER.id, cars_INNER.price, cars_INNER.title FROM cars cars_INNER WHERE (CASE WHEN cars_INNER.title = 'MERCEDES-BENZ SL 600' THEN 0 ELSE 100 END <= CASE WHEN cars.title = 'MERCEDES-BENZ SL 600' THEN 0 ELSE 100 END OR cars_INNER.title = cars.title) AND cars_INNER.price <= cars.price AND ( CASE WHEN cars_INNER.title = 'MERCEDES-BENZ SL 600' THEN 0 ELSE 100 END < CASE WHEN cars.title = 'MERCEDES-BENZ SL 600' THEN 0 ELSE 100 END OR cars_INNER.price < cars.price) )";
            SQLCommon common = new SQLCommon();
            string actual = common.ParsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected.Trim(), actual.Trim(), true, "SQL not built correctly");
        }



        [TestMethod]
        public void TestParserSkyline2DimensionsWithJoin()
        {
            string strPrefSQL = "SELECT cars.id, cars.price, cars.title, colors.name FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID SKYLINE OF colors.name ('red' >> OTHERS EQUAL), cars.price LOW";

            string expected = "SELECT cars.id, cars.price, cars.title, colors.name FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID WHERE NOT EXISTS(SELECT cars_INNER.id, cars_INNER.price, cars_INNER.title, colors_INNER.name FROM cars cars_INNER LEFT OUTER JOIN colors colors_INNER ON cars_INNER.color_id = colors_INNER.ID WHERE (CASE WHEN colors_INNER.name = 'red' THEN 0 ELSE 100 END <= CASE WHEN colors.name = 'red' THEN 0 ELSE 100 END OR colors_INNER.name = colors.name) AND cars_INNER.price <= cars.price AND ( CASE WHEN colors_INNER.name = 'red' THEN 0 ELSE 100 END < CASE WHEN colors.name = 'red' THEN 0 ELSE 100 END OR cars_INNER.price < cars.price) )";

            SQLCommon common = new SQLCommon();
            string actual = common.ParsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected.Trim(), actual.Trim(), true, "SQL not built correctly");
        }


        [TestMethod]
        public void TestParserSkyline2Dimensions()
        {
            string strPrefSQL = "SELECT * FROM cars SKYLINE OF cars.price LOW, cars.mileage LOW ORDER BY price ASC, mileage ASC";

            string expected = "SELECT * FROM cars WHERE NOT EXISTS(SELECT * FROM cars cars_INNER WHERE cars_INNER.price <= cars.price AND cars_INNER.mileage <= cars.mileage AND ( cars_INNER.price < cars.price OR cars_INNER.mileage < cars.mileage) ) ORDER BY price ASC, mileage ASC";
            SQLCommon common = new SQLCommon();
            string actual = common.ParsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected.Trim(), actual.Trim(), true, "SQL not built correctly");



        }


        [TestMethod]
        public void TestParserSkyline2DimensionswithAlias()
        {
            string strPrefSQL = "SELECT * FROM cars t1 SKYLINE OF t1.price LOW, t1.mileage LOW";

            string expected = "SELECT * FROM cars t1 WHERE NOT EXISTS(SELECT * FROM cars t1_INNER WHERE t1_INNER.price <= t1.price AND t1_INNER.mileage <= t1.mileage AND ( t1_INNER.price < t1.price OR t1_INNER.mileage < t1.mileage) )";
            SQLCommon common = new SQLCommon();
            string actual = common.ParsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected.Trim(), actual.Trim(), true, "SQL not built correctly");



        }


        [TestMethod]
        public void TestParserSkyline3Dimensions()
        {
            string strPrefSQL = "SELECT * FROM cars SKYLINE OF cars.price LOW, cars.mileage LOW, cars.horsepower HIGH ORDER BY price ASC, mileage ASC, horsepower DESC";

            string expected = "SELECT * FROM cars WHERE NOT EXISTS(SELECT * FROM cars cars_INNER WHERE cars_INNER.price <= cars.price AND cars_INNER.mileage <= cars.mileage AND cars_INNER.horsepower * -1 <= cars.horsepower * -1 AND ( cars_INNER.price < cars.price OR cars_INNER.mileage < cars.mileage OR cars_INNER.horsepower * -1 < cars.horsepower * -1) ) ORDER BY price ASC, mileage ASC, horsepower DESC";
            SQLCommon common = new SQLCommon();
            string actual = common.ParsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected.Trim(), actual.Trim(), true, "SQL not built correctly");



        }




        [TestMethod]
        public void TestParserSkylinearound()
        {
            string strPrefSQL = "SELECT * FROM cars SKYLINE OF cars.price AROUND 15000, cars.mileage LOW";

            string expected = "SELECT * FROM cars WHERE NOT EXISTS(SELECT * FROM cars cars_INNER WHERE ABS(cars_INNER.price - 15000) <= ABS(cars.price - 15000) AND cars_INNER.mileage <= cars.mileage AND ( ABS(cars_INNER.price - 15000) < ABS(cars.price - 15000) OR cars_INNER.mileage < cars.mileage) )";
            SQLCommon common = new SQLCommon();
            string actual = common.ParsePreferenceSQL(strPrefSQL);

            Assert.AreEqual(expected.Trim(), actual.Trim(), true, "SQL not built correctly");

        }

        [TestMethod]
        [TestCategory("UnitTest")]
        public void TestParserCaseSensitivity()
        {
            var prefSqlQuery = "SELECT * FROM cars SKYLINE OF cars.price AROUND 15000, cars.mileage LOW";
            var expected = "SELECT * FROM cars WHERE NOT EXISTS(SELECT * FROM cars cars_INNER WHERE ABS(cars_INNER.price - 15000) <= ABS(cars.price - 15000) AND cars_INNER.mileage <= cars.mileage AND ( ABS(cars_INNER.price - 15000) < ABS(cars.price - 15000) OR cars_INNER.mileage < cars.mileage) )";
            var common = new SQLCommon();
            var actualNoCaseChange = common.ParsePreferenceSQL(prefSqlQuery);
            var actualUpperCase = common.ParsePreferenceSQL(prefSqlQuery.ToUpper());
            var actualLowerCase = common.ParsePreferenceSQL(prefSqlQuery.ToLower());

            Assert.AreEqual(expected.Trim(), actualNoCaseChange.Trim(), true);
            Assert.AreEqual(expected.Trim(), actualUpperCase.Trim(), true);
            Assert.AreEqual(expected.Trim(), actualLowerCase.Trim(), true);

        }

        [TestMethod]
        [TestCategory("UnitTest")]
        public void TestParserKeywordsInIdentifier()
        {
            var prefSqlQuery = "SELECT TOP 5 Id, FieldA AS IDFROM, FieldB AS IDWHERE, FieldC AS IDTOP " +
                               "FROM table " +
                               "SKYLINE OF id LOW";
            var expected = "SELECT TOP 5 Id, FieldA AS IDFROM, FieldB AS IDWHERE, FieldC AS IDTOP " +
                           "FROM table " +
                           "WHERE NOT EXISTS(SELECT Id, FieldA AS IDFROM, FieldB AS IDWHERE, FieldC AS IDTOP FROM table table_INNER WHERE _INNER.id <= .id AND ( _INNER.id < .id) )";
            var common = new SQLCommon();
            var actual = common.ParsePreferenceSQL(prefSqlQuery);

            Assert.AreEqual(expected.Trim(), actual.Trim(), true);
        }



        void cnnSQL_InfoMessage(object sender, SqlInfoMessageEventArgs e)
        {
            Assert.Fail(e.Message);
        }

    }
}
