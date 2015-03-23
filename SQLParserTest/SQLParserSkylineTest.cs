using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using prefSQL.SQLParser;
using System.Data.SqlClient;
using System.Collections;
using System.Collections.Generic;
using prefSQL.SQLSkyline;
using System.Data;


namespace prefSQL.SQLParserTest
{
    [TestClass]
    public class SQLParserSkylineTest
    {
        private const string strConnection = "Data Source=localhost;Initial Catalog=eCommerce;Integrated Security=True";
        private const string driver = "System.Data.SqlClient";


        private string[] getPreferences()
        {
            string[] strPrefSQL = new string[11];

            //1 numerical preference
            strPrefSQL[0] = "SELECT t1.id AS ID, t1.title, t1.price FROM cars_small t1 SKYLINE OF t1.price LOW";
            //3 numerical preferences
            strPrefSQL[1] = "   SELECT   * FROM cars_small t1 SKYLINE OF t1.price AROUND 10000, t1.mileage LOW, t1.horsepower HIGH";
            //6 numerical preferences with EQUAL STEPS
            strPrefSQL[2] = "SELECT cars_small.price,cars_small.mileage,cars_small.horsepower,cars_small.enginesize,cars_small.consumption,cars_small.doors,colors.name,fuels.name,bodies.name,cars_small.title,makes.name,conditions.name FROM cars_small LEFT OUTER JOIN colors ON cars_small.color_id = colors.ID LEFT OUTER JOIN fuels ON cars_small.fuel_id = fuels.ID LEFT OUTER JOIN bodies ON cars_small.body_id = bodies.ID LEFT OUTER JOIN makes ON cars_small.make_id = makes.ID LEFT OUTER JOIN conditions ON cars_small.condition_id = conditions.ID " +
                "SKYLINE OF cars_small.price LOW 3000 EQUAL, cars_small.mileage LOW 20000 EQUAL, cars_small.horsepower HIGH 20 EQUAL, cars_small.enginesize HIGH 1000 EQUAL, cars_small.consumption LOW 15 EQUAL, cars_small.doors HIGH ";


            //Preference with TOP Keyword
            //1 numerical preferences with TOP Keyword
            strPrefSQL[3] = "  SELECT   TOP   5    t1.title FROM cars_small t1 SKYLINE OF t1.price LOW";

            //3 numerical preferences with TOP Keyword
            strPrefSQL[4] = "SELECT TOP 5 t1.title FROM cars_small t1 SKYLINE OF t1.price LOW, t1.mileage LOW, t1.horsepower HIGH";

            //OTHERS EQUAL at the end
            strPrefSQL[5] = "SELECT t1.id, t1.title AS AutoTitel, t1.price, t1.mileage, colors.name FROM cars_small t1 LEFT OUTER JOIN colors ON t1.color_id = colors.ID SKYLINE OF t1.price LOW, colors.name ('rot' >> 'blau' >> OTHERS EQUAL)";
            //OTHERS EQUAL at the beginning
            strPrefSQL[6] = "SELECT t1.id, t1.title, t1.price, t1.mileage, colors.name FROM cars_small t1 LEFT OUTER JOIN colors ON t1.color_id = colors.ID SKYLINE OF t1.price LOW, colors.name (OTHERS EQUAL >> 'blau')";
            //OTHERS EQUAL in the middle
            strPrefSQL[7] = "SELECT t1.id, t1.title, t1.price, t1.mileage, colors.name FROM cars_small t1 LEFT OUTER JOIN colors ON t1.color_id = colors.ID SKYLINE OF t1.price LOW, colors.name ('rot' >> OTHERS EQUAL >> 'blau')";

            //OTHERS INCOMPARABLE at the end
            strPrefSQL[8] = "SELECT t1.id, t1.title, t1.price, t1.mileage, colors.name FROM cars_small t1 LEFT OUTER JOIN colors ON t1.color_id = colors.ID WHERE (t1.price < 3000) SKYLINE OF t1.price LOW, colors.name ('rot' >> 'blau' >> OTHERS INCOMPARABLE)";
            //OTHERS INCOMPARABLE at the beginning
            strPrefSQL[9] = "SELECT t1.id, t1.title, t1.price, t1.mileage, colors.name FROM cars_small t1 LEFT OUTER JOIN colors ON t1.color_id = colors.ID WHERE (t1.price < 3000) SKYLINE OF t1.price LOW, colors.name (OTHERS INCOMPARABLE >> 'blau' >> 'rot')";
            //OTHERS INCOMPARABLE in the middle
            strPrefSQL[10] = "SELECT t1.id, t1.title, t1.price, t1.mileage, colors.name FROM cars_small t1 LEFT OUTER JOIN colors ON t1.color_id = colors.ID WHERE (t1.price < 3000) SKYLINE OF t1.price LOW, colors.name ('rot' >>  OTHERS INCOMPARABLE >> 'blau')";

            

            //TODO: Statement without explicit OTHERS INCOMPARABLE do not work with hexagon so far
            //2 FIXED INCOMPARABLE values
            /*strPrefSQL[8] = "SELECT t1.id, t1.title, t1.price, t1.mileage, colors.name FROM cars_small t1 LEFT OUTER JOIN colors ON t1.color_id = colors.ID SKYLINE OF t1.price LOW, colors.name ({'blau', 'silber'})";
            //5 FIXED INCOMPARABLE values better than another value
            strPrefSQL[9] = "SELECT t1.id, t1.title, t1.price, t1.mileage, colors.name FROM cars_small t1 LEFT OUTER JOIN colors ON t1.color_id = colors.ID WHERE (t1.price = 2400 OR t1.price = 900) SKYLINE OF t1.price LOW, colors.name ({'blau', 'silber', 'schwarz', 'rot', 'pink'} >> 'grau')";
            //4 FIXED INCOMPARABLE values in the middle
            strPrefSQL[10] = "SELECT t1.id, t1.title, t1.price, t1.mileage, colors.name FROM cars_small t1 LEFT OUTER JOIN colors ON t1.color_id = colors.ID WHERE (t1.price = 2400 OR t1.price = 900) SKYLINE OF t1.price LOW, colors.name ('schwarz' >> {'blau', 'silber', 'rot', 'pink'} >> 'grau')";
            */

            //Preferene with Step Level Equal
            //strPrefSQL[12] = "SELECT t1.id AS ID, t1.title, t1.price FROM cars_small t1 SKYLINE OF t1.price LOW 1000 INCOMPARABLE, t1.mileage LOW";

            //WITHOUT OTHERS --> This means that tuples with other values are assumed to be incomparable
            //strPrefSQL[14] = "SELECT c.id AS ID FROM cars_small c LEFT OUTER JOIN bodies b ON c.body_id = b.ID SKYLINE OF c.price LOW, b.name ('Bus' >> 'Kleinwagen')";

            //TODO: Does not work with BNL and Hexagon
            //Numerical preferences with INCOMPARABLE STEPS
            /*strPrefSQL[15] = "SELECT cars_small.price,cars_small.mileage,cars_small.horsepower,cars_small.enginesize,cars_small.consumption,cars_small.doors,colors.name,fuels.name,bodies.name,cars_small.title,makes.name,conditions.name FROM cars_small LEFT OUTER JOIN colors ON cars_small.color_id = colors.ID LEFT OUTER JOIN fuels ON cars_small.fuel_id = fuels.ID LEFT OUTER JOIN bodies ON cars_small.body_id = bodies.ID LEFT OUTER JOIN makes ON cars_small.make_id = makes.ID LEFT OUTER JOIN conditions ON cars_small.condition_id = conditions.ID " +
                "SKYLINE OF cars_small.price LOW 3000 INCOMPARABLE, cars_small.mileage LOW 20000 EQUAL, cars_small.horsepower HIGH 20 EQUAL, cars_small.enginesize HIGH 1000 EQUAL, cars_small.consumption LOW 15 EQUAL, cars_small.doors HIGH ";
            */


            return strPrefSQL;
        }

        /**
         * This test checks if the algorithms return the same amount of tupels for different prefSQL statements
         * At the moment it contains the BNL, nativeSQL and Hexagon algorithm. It is intendend to add the D&Q as soon
         * as it works
         * 
         * */
        [TestMethod]
        public void TestSKYLINEAmountOfTupels_MSSQLCLR()
        {
            string[] strPrefSQL = getPreferences();
            
            
            for (int i = 0; i <= strPrefSQL.GetUpperBound(0); i++)
            {

                SQLCommon common = new SQLCommon();
                common.SkylineType = new SkylineSQL();
                string sqlNative = common.parsePreferenceSQL(strPrefSQL[i]);
                common.SkylineType = new SkylineBNL();
                string sqlBNL = common.parsePreferenceSQL(strPrefSQL[i]);
                common.SkylineType = new SkylineBNLSort();
                string sqlBNLSort = common.parsePreferenceSQL(strPrefSQL[i]);
                common.SkylineType = new SkylineHexagon();
                string sqlHexagon = common.parsePreferenceSQL(strPrefSQL[i]);
                //D&Q does not run with CLR
                common.SkylineType = new SkylineDQ();
                string sqlDQ = common.parsePreferenceSQL(strPrefSQL[i]);

                int amountOfTupelsBNL = 0;
                int amountOfTupelsBNLSort = 0;
                int amountOfTupelsSQL = 0;
                int amountOfTupelsHexagon = 0;
                int amountOfTupelsDQ = 0;

                SqlConnection cnnSQL = new SqlConnection(strConnection);
                cnnSQL.InfoMessage += cnnSQL_InfoMessage;
                try
                {
                    cnnSQL.Open();

                    //Native
                    SqlCommand sqlCommand = new SqlCommand(sqlNative, cnnSQL);
                    SqlDataReader sqlReader = sqlCommand.ExecuteReader();

                    if (sqlReader.HasRows)
                    {
                        while (sqlReader.Read())
                        {
                            amountOfTupelsSQL++;
                        }
                    }
                    sqlReader.Close();

                    //BNL
                    sqlCommand = new SqlCommand(sqlBNL, cnnSQL);
                    sqlReader = sqlCommand.ExecuteReader();

                    if (sqlReader.HasRows)
                    {
                        while (sqlReader.Read())
                        {
                            amountOfTupelsBNL++;
                        }
                    }
                    sqlReader.Close();

                    //BNLSort
                    sqlCommand = new SqlCommand(sqlBNLSort, cnnSQL);
                    sqlReader = sqlCommand.ExecuteReader();

                    if (sqlReader.HasRows)
                    {
                        while (sqlReader.Read())
                        {
                            amountOfTupelsBNLSort++;
                        }
                    }
                    sqlReader.Close();


                    //Hexagon
                    sqlCommand = new SqlCommand(sqlHexagon, cnnSQL);
                    sqlReader = sqlCommand.ExecuteReader();

                    if (sqlReader.HasRows)
                    {
                        while (sqlReader.Read())
                        {
                            amountOfTupelsHexagon++;
                        }
                    }
                    sqlReader.Close();

                    //D&Q
                    //D&Q does not work with incomparable tuples
                    if (i < 7)
                    {


                        sqlCommand = new SqlCommand(sqlDQ, cnnSQL);
                        sqlReader = sqlCommand.ExecuteReader();

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


                //Check tuples (every algorithm should deliver the same amount of tuples)
                Assert.AreEqual(amountOfTupelsSQL, amountOfTupelsBNLSort, 0, "BNLSort Amount of tupels in query " + i + "do not match");
                Assert.AreEqual(amountOfTupelsSQL, amountOfTupelsBNL, 0, "BNL Amount of tupels in query " + i + "do not match");
                Assert.AreEqual(amountOfTupelsSQL, amountOfTupelsHexagon, 0, "Hexagon Amount of tupels in query " + i + "do not match");

                //D&Q does not work with incomparable tuples
                if (i < 7)
                {
                    Assert.AreEqual(amountOfTupelsSQL, amountOfTupelsDQ, 0, "Amount of tupels in query " + i + "do not match");
                }
                
            }
        }


        /**
         * This test checks if the algorithms return the same amount of tupels for different prefSQL statements
         * At the moment it contains the BNL, nativeSQL and Hexagon algorithm. It is intendend to add the D&Q as soon
         * as it works
         * 
         * */
        [TestMethod]
        public void TestSKYLINEAmountOfTupels_DataTable()
        {
            string[] strPrefSQL = getPreferences();

            for (int i = 0; i <= strPrefSQL.GetUpperBound(0); i++)
            {
                
                SQLCommon common = new SQLCommon();
                common.SkylineType = new SkylineSQL();
                DataTable dtNative = common.parseAndExecutePrefSQL(strConnection, driver, strPrefSQL[i]);
                common.SkylineType = new SkylineBNL();
                DataTable dtBNL = common.parseAndExecutePrefSQL(strConnection, driver, strPrefSQL[i]);
                common.SkylineType = new SkylineBNLSort();
                DataTable dtBNLSort = common.parseAndExecutePrefSQL(strConnection, driver, strPrefSQL[i]);
                common.SkylineType = new SkylineHexagon();
                DataTable dtHexagon = common.parseAndExecutePrefSQL(strConnection, driver, strPrefSQL[i]);
                DataTable dtDQ = new DataTable();

                //D&Q does not work with incomparable tuples
                if (i < 7)
                {
                    common.SkylineType = new SkylineDQ();    
                    dtDQ = common.parseAndExecutePrefSQL(strConnection, driver, strPrefSQL[i]);
                }
                

                //Check tuples (every algorithm should deliver the same amount of tuples)
                Assert.AreEqual(dtNative.Rows.Count, dtBNL.Rows.Count, 0, "BNL Amount of tupels in query " + i + " do not match");
                Assert.AreEqual(dtNative.Rows.Count, dtBNLSort.Rows.Count, 0, "BNLSort Amount of tupels in query " + i + " do not match");
                Assert.AreEqual(dtNative.Rows.Count, dtHexagon.Rows.Count, 0, "Hexagon Amount of tupels in query " + i + " do not match");
                //D&Q does not work with incomparable tuples
                if(i < 7)
                {
                    Assert.AreEqual(dtNative.Rows.Count, dtDQ.Rows.Count, 0, "D&Q Amount of tupels in query " + i + " do not match");
                }
                
            }
        }



        [TestMethod]
        public void TestSKYLINEMultipleLevels()
        {
            string strSQL = "SELECT t1.id, t1.price, t1.mileage FROM cars_small t1 ";
            string strPreferences = " SKYLINE OF t1.price LOW, t1.mileage LOW";
            SQLCommon common = new SQLCommon();

            SqlConnection cnnSQL = new SqlConnection(strConnection);
            cnnSQL.InfoMessage += cnnSQL_InfoMessage;
            try
            {
                cnnSQL.Open();


                //Tree Algorithm
                common.SkylineType = new MultipleSkylineBNL();
                common.SkylineUpToLevel = 3;
                string sqlTree = common.parsePreferenceSQL(strSQL + strPreferences);
                ArrayList levelRecordsTree = new ArrayList(); ;
                SqlCommand sqlCommand = new SqlCommand(sqlTree, cnnSQL);
                SqlDataReader sqlReader = sqlCommand.ExecuteReader();

                if (sqlReader.HasRows)
                {
                    while (sqlReader.Read())
                    {
                        int level = (int)sqlReader["level"];
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
                sqlReader.Close();


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
                    string sqlBNLSort = common.parsePreferenceSQL(strSQL + strIDs + strPreferences);
                    
                    sqlCommand = new SqlCommand(sqlBNLSort, cnnSQL);
                    sqlReader = sqlCommand.ExecuteReader();

                    if (sqlReader.HasRows)
                    {
                        while (sqlReader.Read())
                        {
                            listIDs.Add(Int32.Parse(sqlReader["id"].ToString()));

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
                        sqlReader.Close();
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
        public void TestSKYLINEShowSkylineAttributes()
        {
            string strPrefSQL = "SELECT * FROM cars SKYLINE OF cars.price LOW, cars.mileage LOW, cars.horsepower HIGH";

            string expected = "SELECT * , DENSE_RANK() OVER (ORDER BY cars.price) AS SkylineAttributecars_price, DENSE_RANK() OVER (ORDER BY cars.mileage) AS SkylineAttributecars_mileage, DENSE_RANK() OVER (ORDER BY cars.horsepower * -1) AS SkylineAttributecars_horsepower FROM cars WHERE NOT EXISTS(SELECT * , DENSE_RANK() OVER (ORDER BY cars_INNER.price) AS SkylineAttributecars_price, DENSE_RANK() OVER (ORDER BY cars_INNER.mileage) AS SkylineAttributecars_mileage, DENSE_RANK() OVER (ORDER BY cars_INNER.horsepower * -1) AS SkylineAttributecars_horsepower FROM cars cars_INNER WHERE cars_INNER.price <= cars.price AND cars_INNER.mileage <= cars.mileage AND cars_INNER.horsepower * -1 <= cars.horsepower * -1 AND ( cars_INNER.price < cars.price OR cars_INNER.mileage < cars.mileage OR cars_INNER.horsepower * -1 < cars.horsepower * -1) ) ";
            SQLCommon common = new SQLCommon();
            common.ShowSkylineAttributes = true;
            string actual = common.parsePreferenceSQL(strPrefSQL);

            Assert.AreEqual(expected.Trim(), actual.Trim(), true, "SQL not built correctly");
        }



        [TestMethod]
        public void TestSKYLINE_LOW_With_Level()
        {
            string strPrefSQL = "SELECT cars.id, cars.title, cars.Price FROM cars SKYLINE OF cars.price LOW 1000 EQUAL, cars.mileage LOW";

            string expected = "SELECT cars.id, cars.title, cars.Price FROM cars WHERE NOT EXISTS(SELECT cars_INNER.id, cars_INNER.title, cars_INNER.Price FROM cars cars_INNER WHERE cars_INNER.price / 1000 <= cars.price / 1000 AND cars_INNER.mileage <= cars.mileage AND ( cars_INNER.price / 1000 < cars.price / 1000 OR cars_INNER.mileage < cars.mileage) ) ";
            SQLCommon common = new SQLCommon();
            string actual = common.parsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected.Trim(), actual.Trim(), true, "SQL not built correctly");
        }


        [TestMethod]
        public void TestSKYLINEFavourRot()
        {
            string strPrefSQL = "SELECT cars.id, cars.title, cars.Price, colors.Name FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID SKYLINE OF cars.price LOW, colors.name FAVOUR 'rot'";

            string expected = "SELECT cars.id, cars.title, cars.Price, colors.Name FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID WHERE NOT EXISTS(SELECT cars_INNER.id, cars_INNER.title, cars_INNER.Price, colors_INNER.Name FROM cars cars_INNER LEFT OUTER JOIN colors colors_INNER ON cars_INNER.color_id = colors_INNER.ID WHERE cars_INNER.price <= cars.price AND CASE WHEN colors_INNER.name = 'rot' THEN 1 ELSE 2 END <= CASE WHEN colors.name = 'rot' THEN 1 ELSE 2 END AND ( cars_INNER.price < cars.price OR CASE WHEN colors_INNER.name = 'rot' THEN 1 ELSE 2 END < CASE WHEN colors.name = 'rot' THEN 1 ELSE 2 END) )";
            SQLCommon common = new SQLCommon();
            string actual = common.parsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected.Trim(), actual.Trim(), true, "SQL not built correctly");
        }



        [TestMethod]
        public void TestSKYLINEWithUnconventialJOIN()
        {
            string strPrefSQL = "SELECT cars.id, cars.title, cars.price, colors.name FROM cars, colors WHERE cars.Color_Id = colors.Id SKYLINE OF cars.price LOW, colors.name ('grau' >> 'rot')";

            string expected = "SELECT cars.id, cars.title, cars.price, colors.name FROM cars, colors WHERE cars.Color_Id = colors.Id AND NOT EXISTS(SELECT cars_INNER.id, cars_INNER.title, cars_INNER.price, colors_INNER.name FROM cars cars_INNER, colors colors_INNER WHERE cars_INNER.Color_Id = colors_INNER.Id  AND cars_INNER.price <= cars.price AND (CASE WHEN colors_INNER.name = 'grau' THEN 0 WHEN colors_INNER.name = 'rot' THEN 100 END <= CASE WHEN colors.name = 'grau' THEN 0 WHEN colors.name = 'rot' THEN 100 END OR colors_INNER.name = colors.name) AND ( cars_INNER.price < cars.price OR CASE WHEN colors_INNER.name = 'grau' THEN 0 WHEN colors_INNER.name = 'rot' THEN 100 END < CASE WHEN colors.name = 'grau' THEN 0 WHEN colors.name = 'rot' THEN 100 END) )";
            SQLCommon common = new SQLCommon();
            string actual = common.parsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected.Trim(), actual.Trim(), true, "SQL not built correctly");
        }


        [TestMethod]
        public void TestSKYLINEWithWHEREClause()
        {
            string strPrefSQL = "SELECT cars.id, cars.title, cars.price, cars.mileage FROM cars WHERE cars.price > 10000 SKYLINE OF cars.price LOW, cars.mileage low";

            string expected = "SELECT cars.id, cars.title, cars.price, cars.mileage FROM cars WHERE cars.price > 10000 AND NOT EXISTS(SELECT cars_INNER.id, cars_INNER.title, cars_INNER.price, cars_INNER.mileage FROM cars cars_INNER WHERE cars_INNER.price > 10000  AND cars_INNER.price <= cars.price AND cars_INNER.mileage <= cars.mileage AND ( cars_INNER.price < cars.price OR cars_INNER.mileage < cars.mileage) )";
            SQLCommon common = new SQLCommon();
            string actual = common.parsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected.Trim(), actual.Trim(), true, "SQL not built correctly");
        }




        [TestMethod]
        public void TestSKYLINETOPKeyword()
        {
            string strPrefSQL = "SELECT TOP 5 cars.id, cars.title, cars.Price, colors.Name FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID SKYLINE OF cars.price LOW, colors.name ('pink' >> {'rot', 'schwarz'} >> 'beige' == 'gelb')";

            string expected = "SELECT TOP 5 cars.id, cars.title, cars.Price, colors.Name FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID WHERE NOT EXISTS(SELECT cars_INNER.id, cars_INNER.title, cars_INNER.Price, colors_INNER.Name FROM cars cars_INNER LEFT OUTER JOIN colors colors_INNER ON cars_INNER.color_id = colors_INNER.ID WHERE cars_INNER.price <= cars.price AND (CASE WHEN colors_INNER.name = 'pink' THEN 0 WHEN colors_INNER.name IN ('rot','schwarz') THEN 101 WHEN colors_INNER.name = 'beige' THEN 200 WHEN colors_INNER.name = 'gelb' THEN 200 END <= CASE WHEN colors.name = 'pink' THEN 0 WHEN colors.name IN ('rot','schwarz') THEN 100 WHEN colors.name = 'beige' THEN 200 WHEN colors.name = 'gelb' THEN 200 END OR colors_INNER.name = colors.name) AND ( cars_INNER.price < cars.price OR CASE WHEN colors_INNER.name = 'pink' THEN 0 WHEN colors_INNER.name IN ('rot','schwarz') THEN 101 WHEN colors_INNER.name = 'beige' THEN 200 WHEN colors_INNER.name = 'gelb' THEN 200 END < CASE WHEN colors.name = 'pink' THEN 0 WHEN colors.name IN ('rot','schwarz') THEN 100 WHEN colors.name = 'beige' THEN 200 WHEN colors.name = 'gelb' THEN 200 END) )";
            SQLCommon common = new SQLCommon();
            string actual = common.parsePreferenceSQL(strPrefSQL);

            // assert
            Assert.AreEqual(expected.Trim(), actual.Trim(), true, "SQL not built correctly");
        }


        [TestMethod]
        public void TestSKYLINE2DimensionsJoinMultipleAccumulation()
        {
            string strPrefSQL = "SELECT cars.id, cars.title, cars.Price, colors.Name FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID SKYLINE OF cars.price LOW, colors.name ('pink' >> {'rot', 'schwarz'} >> 'beige' == 'gelb') ORDER BY cars.price ASC, colors.name('pink'>>{'rot','schwarz'}>>'beige'=='gelb')";

            string expected = "SELECT cars.id, cars.title, cars.Price, colors.Name FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID WHERE NOT EXISTS(SELECT cars_INNER.id, cars_INNER.title, cars_INNER.Price, colors_INNER.Name FROM cars cars_INNER LEFT OUTER JOIN colors colors_INNER ON cars_INNER.color_id = colors_INNER.ID WHERE cars_INNER.price <= cars.price AND (CASE WHEN colors_INNER.name = 'pink' THEN 0 WHEN colors_INNER.name IN ('rot','schwarz') THEN 101 WHEN colors_INNER.name = 'beige' THEN 200 WHEN colors_INNER.name = 'gelb' THEN 200 END <= CASE WHEN colors.name = 'pink' THEN 0 WHEN colors.name IN ('rot','schwarz') THEN 100 WHEN colors.name = 'beige' THEN 200 WHEN colors.name = 'gelb' THEN 200 END OR colors_INNER.name = colors.name) AND ( cars_INNER.price < cars.price OR CASE WHEN colors_INNER.name = 'pink' THEN 0 WHEN colors_INNER.name IN ('rot','schwarz') THEN 101 WHEN colors_INNER.name = 'beige' THEN 200 WHEN colors_INNER.name = 'gelb' THEN 200 END < CASE WHEN colors.name = 'pink' THEN 0 WHEN colors.name IN ('rot','schwarz') THEN 100 WHEN colors.name = 'beige' THEN 200 WHEN colors.name = 'gelb' THEN 200 END) ) ORDER BY cars.price ASC, CASE WHEN colors.name = 'pink' THEN 0 WHEN colors.name IN ('rot','schwarz') THEN 100 WHEN colors.name = 'beige' THEN 200 WHEN colors.name = 'gelb' THEN 200 END ASC";
            SQLCommon common = new SQLCommon();
            string actual = common.parsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected.Trim(), actual.Trim(), true, "SQL not built correctly");
        }







        [TestMethod]
        public void TestSKYLINE2DimensionsJoinOthersAccumulation()
        {
            string strPrefSQL = "SELECT cars.id, cars.title, cars.Price, colors.Name FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID SKYLINE OF cars.price LOW, colors.name ('türkis' >> 'gelb' >> OTHERS INCOMPARABLE)";

            string expected = "SELECT cars.id, cars.title, cars.Price, colors.Name FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID WHERE NOT EXISTS(SELECT cars_INNER.id, cars_INNER.title, cars_INNER.Price, colors_INNER.Name FROM cars cars_INNER LEFT OUTER JOIN colors colors_INNER ON cars_INNER.color_id = colors_INNER.ID WHERE cars_INNER.price <= cars.price AND (CASE WHEN colors_INNER.name = 'türkis' THEN 0 WHEN colors_INNER.name = 'gelb' THEN 100 ELSE 201 END <= CASE WHEN colors.name = 'türkis' THEN 0 WHEN colors.name = 'gelb' THEN 100 ELSE 200 END OR colors_INNER.name = colors.name) AND ( cars_INNER.price < cars.price OR CASE WHEN colors_INNER.name = 'türkis' THEN 0 WHEN colors_INNER.name = 'gelb' THEN 100 ELSE 201 END < CASE WHEN colors.name = 'türkis' THEN 0 WHEN colors.name = 'gelb' THEN 100 ELSE 200 END) )";
            SQLCommon common = new SQLCommon();
            string actual = common.parsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected.Trim(), actual.Trim(), true, "SQL not built correctly");
        }


        [TestMethod]
        public void TestSKYLINE2DimensionsJoinNoOthers()
        {
            string strPrefSQL = "SELECT cars.id, cars.title, cars.Price, colors.Name FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID SKYLINE OF cars.price LOW, colors.name ('pink' >> 'rot' == 'schwarz' >> 'beige' == 'gelb')";

            string expected = "SELECT cars.id, cars.title, cars.Price, colors.Name FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID WHERE NOT EXISTS(SELECT cars_INNER.id, cars_INNER.title, cars_INNER.Price, colors_INNER.Name FROM cars cars_INNER LEFT OUTER JOIN colors colors_INNER ON cars_INNER.color_id = colors_INNER.ID WHERE cars_INNER.price <= cars.price AND (CASE WHEN colors_INNER.name = 'pink' THEN 0 WHEN colors_INNER.name = 'rot' THEN 100 WHEN colors_INNER.name = 'schwarz' THEN 100 WHEN colors_INNER.name = 'beige' THEN 200 WHEN colors_INNER.name = 'gelb' THEN 200 END <= CASE WHEN colors.name = 'pink' THEN 0 WHEN colors.name = 'rot' THEN 100 WHEN colors.name = 'schwarz' THEN 100 WHEN colors.name = 'beige' THEN 200 WHEN colors.name = 'gelb' THEN 200 END OR colors_INNER.name = colors.name) AND ( cars_INNER.price < cars.price OR CASE WHEN colors_INNER.name = 'pink' THEN 0 WHEN colors_INNER.name = 'rot' THEN 100 WHEN colors_INNER.name = 'schwarz' THEN 100 WHEN colors_INNER.name = 'beige' THEN 200 WHEN colors_INNER.name = 'gelb' THEN 200 END < CASE WHEN colors.name = 'pink' THEN 0 WHEN colors.name = 'rot' THEN 100 WHEN colors.name = 'schwarz' THEN 100 WHEN colors.name = 'beige' THEN 200 WHEN colors.name = 'gelb' THEN 200 END) )";
            SQLCommon common = new SQLCommon();
            string actual = common.parsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected.Trim(), actual.Trim(), true, "SQL not built correctly");
        }



        [TestMethod]
        public void TestSKYLINE2DimensionsNoJoin()
        {
            string strPrefSQL = "SELECT cars.id, cars.price, cars.title FROM cars SKYLINE OF cars.title ('MERCEDES-BENZ SL 600' >> OTHERS EQUAL), cars.price LOW";

            string expected = "SELECT cars.id, cars.price, cars.title FROM cars WHERE NOT EXISTS(SELECT cars_INNER.id, cars_INNER.price, cars_INNER.title FROM cars cars_INNER WHERE (CASE WHEN cars_INNER.title = 'MERCEDES-BENZ SL 600' THEN 0 ELSE 100 END <= CASE WHEN cars.title = 'MERCEDES-BENZ SL 600' THEN 0 ELSE 100 END OR cars_INNER.title = cars.title) AND cars_INNER.price <= cars.price AND ( CASE WHEN cars_INNER.title = 'MERCEDES-BENZ SL 600' THEN 0 ELSE 100 END < CASE WHEN cars.title = 'MERCEDES-BENZ SL 600' THEN 0 ELSE 100 END OR cars_INNER.price < cars.price) )";
            SQLCommon common = new SQLCommon();
            string actual = common.parsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected.Trim(), actual.Trim(), true, "SQL not built correctly");
        }



        [TestMethod]
        public void TestSKYLINE2DimensionsWithJoin()
        {
            string strPrefSQL = "SELECT cars.id, cars.price, cars.title, colors.name FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID SKYLINE OF colors.name ('rot' >> OTHERS EQUAL), cars.price LOW";

            string expected = "SELECT cars.id, cars.price, cars.title, colors.name FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID WHERE NOT EXISTS(SELECT cars_INNER.id, cars_INNER.price, cars_INNER.title, colors_INNER.name FROM cars cars_INNER LEFT OUTER JOIN colors colors_INNER ON cars_INNER.color_id = colors_INNER.ID WHERE (CASE WHEN colors_INNER.name = 'rot' THEN 0 ELSE 100 END <= CASE WHEN colors.name = 'rot' THEN 0 ELSE 100 END OR colors_INNER.name = colors.name) AND cars_INNER.price <= cars.price AND ( CASE WHEN colors_INNER.name = 'rot' THEN 0 ELSE 100 END < CASE WHEN colors.name = 'rot' THEN 0 ELSE 100 END OR cars_INNER.price < cars.price) )";

            SQLCommon common = new SQLCommon();
            string actual = common.parsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected.Trim(), actual.Trim(), true, "SQL not built correctly");
        }


        [TestMethod]
        public void TestSKYLINE2Dimensions()
        {
            string strPrefSQL = "SELECT * FROM cars SKYLINE OF cars.price LOW, cars.mileage LOW ORDER BY price ASC, mileage ASC";

            string expected = "SELECT * FROM cars WHERE NOT EXISTS(SELECT * FROM cars cars_INNER WHERE cars_INNER.price <= cars.price AND cars_INNER.mileage <= cars.mileage AND ( cars_INNER.price < cars.price OR cars_INNER.mileage < cars.mileage) ) ORDER BY price ASC, mileage ASC";
            SQLCommon common = new SQLCommon();
            string actual = common.parsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected.Trim(), actual.Trim(), true, "SQL not built correctly");



        }


        [TestMethod]
        public void TestSKYLINE2DimensionswithALIAS()
        {
            string strPrefSQL = "SELECT * FROM cars t1 SKYLINE OF t1.price LOW, t1.mileage LOW";

            string expected = "SELECT * FROM cars t1 WHERE NOT EXISTS(SELECT * FROM cars t1_INNER WHERE t1_INNER.price <= t1.price AND t1_INNER.mileage <= t1.mileage AND ( t1_INNER.price < t1.price OR t1_INNER.mileage < t1.mileage) )";
            SQLCommon common = new SQLCommon();
            string actual = common.parsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected.Trim(), actual.Trim(), true, "SQL not built correctly");



        }


        [TestMethod]
        public void TestSKYLINE3Dimensions()
        {
            string strPrefSQL = "SELECT * FROM cars SKYLINE OF cars.price LOW, cars.mileage LOW, cars.horsepower HIGH ORDER BY price ASC, mileage ASC, horsepower DESC";

            string expected = "SELECT * FROM cars WHERE NOT EXISTS(SELECT * FROM cars cars_INNER WHERE cars_INNER.price <= cars.price AND cars_INNER.mileage <= cars.mileage AND cars_INNER.horsepower * -1 <= cars.horsepower * -1 AND ( cars_INNER.price < cars.price OR cars_INNER.mileage < cars.mileage OR cars_INNER.horsepower * -1 < cars.horsepower * -1) ) ORDER BY price ASC, mileage ASC, horsepower DESC";
            SQLCommon common = new SQLCommon();
            string actual = common.parsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected.Trim(), actual.Trim(), true, "SQL not built correctly");



        }




        [TestMethod]
        public void TestSKYLINEAROUND()
        {
            string strPrefSQL = "SELECT * FROM cars SKYLINE OF cars.price AROUND 15000, cars.mileage LOW";

            string expected = "SELECT * FROM cars WHERE NOT EXISTS(SELECT * FROM cars cars_INNER WHERE ABS(cars_INNER.price - 15000) <= ABS(cars.price - 15000) AND cars_INNER.mileage <= cars.mileage AND ( ABS(cars_INNER.price - 15000) < ABS(cars.price - 15000) OR cars_INNER.mileage < cars.mileage) )";
            SQLCommon common = new SQLCommon();
            string actual = common.parsePreferenceSQL(strPrefSQL);

            Assert.AreEqual(expected.Trim(), actual.Trim(), true, "SQL not built correctly");

        }


        void cnnSQL_InfoMessage(object sender, SqlInfoMessageEventArgs e)
        {
            Assert.Fail(e.Message);
        }

    }
}
