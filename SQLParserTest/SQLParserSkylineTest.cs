using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using prefSQL.SQLParser;
using System.Data.SqlClient;
using System.Collections;
using System.Collections.Generic;


namespace prefSQL.SQLParserTest
{
    [TestClass]
    public class SQLParserSkylineTest
    {
        private const string strConnection = "Data Source=localhost;Initial Catalog=eCommerce;Integrated Security=True";

        /**
         * If only one skyline attribute is available --> Show all records for each algorithm
         * 
         */
        [TestMethod]
        public void TestSKYLINEOneAttribute()
        {
            string sqlAll = "SELECT COUNT(*) FROM cars_small";
            string strPrefSQL = "SELECT c.id AS ID FROM Cars_small c LEFT OUTER JOIN bodies b ON c.body_id = b.ID SKYLINE OF b.name ('Bus' >> 'Kleinwagen')";


            SQLCommon common = new SQLCommon();
            common.SkylineType = SQLCommon.Algorithm.NativeSQL;
            string sqlNative = common.parsePreferenceSQL(strPrefSQL);
            common.SkylineType = SQLCommon.Algorithm.BNLSort;
            string sqlBNL = common.parsePreferenceSQL(strPrefSQL);


            int amountOfTupels = 0;
            int amountOfTupelsBNL = 0;
            int amountOfTupelsSQL = 0;

            SqlConnection cnnSQL = new SqlConnection(strConnection);
            cnnSQL.InfoMessage += cnnSQL_InfoMessage;
            try
            {
                cnnSQL.Open();


                //All
                SqlCommand sqlCommand = new SqlCommand(sqlAll, cnnSQL);
                SqlDataReader sqlReader = sqlCommand.ExecuteReader();

                if (sqlReader.HasRows)
                {
                    while (sqlReader.Read())
                    {
                        amountOfTupels = sqlReader.GetInt32(0);
                        break;
                    }
                }
                sqlReader.Close();

                //Native
                sqlCommand = new SqlCommand(sqlNative, cnnSQL);
                sqlReader = sqlCommand.ExecuteReader();

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


                cnnSQL.Close();
            }
            catch (Exception ex)
            {
                Assert.Fail("Connection failed:" + ex.Message);
            }


            Assert.AreEqual(amountOfTupels, amountOfTupelsSQL, 0, "Amount of tupels does not match");
            Assert.AreEqual(amountOfTupels, amountOfTupelsBNL, 0, "Amount of tupels does not match");
        }


        [TestMethod]
        public void TestSKYLINEAmountsOfTupelsWithoutOTHERS()
        {

            string strPrefSQL = "SELECT t1.id, t1.title, t1.price, t1.mileage, colors.name FROM cars_small t1 " +
                "LEFT OUTER JOIN colors ON t1.color_id = colors.ID " +
                "SKYLINE OF t1.price LOW, colors.name ('rot' >> 'blau')";

            SQLCommon common = new SQLCommon();
            common.SkylineType = SQLCommon.Algorithm.NativeSQL;
            string sqlNative = common.parsePreferenceSQL(strPrefSQL);
            common.SkylineType = SQLCommon.Algorithm.BNLSort;
            string sqlBNL = common.parsePreferenceSQL(strPrefSQL);


            int amountOfTupelsNative = 0;
            int amountOfTupelsBNL = 0;

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
                        amountOfTupelsNative++;
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


                cnnSQL.Close();
            }
            catch (Exception ex)
            {
                Assert.Fail("Connection failed:" + ex.Message);
            }

            Assert.AreEqual(amountOfTupelsNative, amountOfTupelsBNL, 0, "Amount of tupels does not match");
        }


        [TestMethod]
        public void TestSKYLINEAmountsOfTupelsOTHERSIncomparable()
        {
             string strPrefSQL = "SELECT t1.id, t1.title, t1.price, t1.mileage, colors.name FROM cars_small t1 " +
                "LEFT OUTER JOIN colors ON t1.color_id = colors.ID " +
                "SKYLINE OF t1.price LOW, colors.name ('rot' >> 'blau' >> OTHERS INCOMPARABLE)";

            SQLCommon common = new SQLCommon();
            common.SkylineType = SQLCommon.Algorithm.NativeSQL;
            string sqlNative = common.parsePreferenceSQL(strPrefSQL);
            common.SkylineType = SQLCommon.Algorithm.BNLSort;
            string sqlBNL = common.parsePreferenceSQL(strPrefSQL);


            int amountOfTupelsNative = 0;
            int amountOfTupelsBNL = 0;

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
                        amountOfTupelsNative++;
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


                cnnSQL.Close();
            }
            catch (Exception ex)
            {
                Assert.Fail("Connection failed:" + ex.Message);
            }

            Assert.AreEqual(amountOfTupelsNative, amountOfTupelsBNL, 0, "Amount of tupels does not match");
        }


        

        [TestMethod]
        public void TestSKYLINEAmountsOfTupelsModel()
        {

            string strPrefSQL = "SELECT c.id AS ID FROM Cars_small c LEFT OUTER JOIN bodies b ON c.body_id = b.ID SKYLINE OF c.price LOW, b.name ('Bus' >> 'Kleinwagen')";

            SQLCommon common = new SQLCommon();
            common.SkylineType = SQLCommon.Algorithm.NativeSQL;
            string sqlNative = common.parsePreferenceSQL(strPrefSQL);
            common.SkylineType = SQLCommon.Algorithm.BNLSort;
            string sqlBNL = common.parsePreferenceSQL(strPrefSQL);


            int amountOfTupelsNative = 0;
            int amountOfTupelsBNL = 0;

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
                        amountOfTupelsNative++;
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


                cnnSQL.Close();
            }
            catch (Exception ex)
            {
                Assert.Fail("Connection failed:" + ex.Message);
            }

            Assert.AreEqual(amountOfTupelsNative, amountOfTupelsBNL, 0, "Amount of tupels does not match");
        }


        [TestMethod]
        public void TestSKYLINEMultipleLevels()
        {
            string strSQL = "SELECT t1.id, t1.price, t1.mileage FROM cars_small t1 ";
            string strPreferences = " SKYLINE OF t1.price LOW, t1.mileage LOW";
            //string strPreferences = " SKYLINE OF t1.price LOW 60000, t1.horsepower HIGH 80";
            SQLCommon common = new SQLCommon();
            

            SqlConnection cnnSQL = new SqlConnection(strConnection);
            cnnSQL.InfoMessage += cnnSQL_InfoMessage;
            try
            {
                cnnSQL.Open();


                //Tree Algorithm
                common.SkylineType = SQLCommon.Algorithm.MultipleBNL;
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
                common.SkylineType = SQLCommon.Algorithm.BNLSort;
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

            string expected = "SELECT * , cars.price AS SkylineAttributeprice, cars.mileage AS SkylineAttributemileage, cars.horsepower*-1 AS SkylineAttributehorsepower FROM cars WHERE NOT EXISTS(SELECT * , cars_INNER.price AS SkylineAttributeprice, cars_INNER.mileage AS SkylineAttributemileage, cars_INNER.horsepower*-1 AS SkylineAttributehorsepower FROM cars cars_INNER WHERE cars_INNER.price <= cars.price AND cars_INNER.mileage <= cars.mileage AND cars_INNER.horsepower >= cars.horsepower AND ( cars_INNER.price < cars.price OR cars_INNER.mileage < cars.mileage OR cars_INNER.horsepower > cars.horsepower) )";
            SQLCommon common = new SQLCommon();
            common.ShowSkylineAttributes = true;
            string actual = common.parsePreferenceSQL(strPrefSQL);

            Assert.AreEqual(expected.Trim(), actual.Trim(), true, "SQL not built correctly");
        }



        [TestMethod]
        public void TestSKYLINEBNLDateField()
        {

            string strPrefSQL = "SELECT cars_small.price,cars_small.mileage,cars_small.registration FROM cars_small SKYLINE OF cars_small.price LOW, cars_small.mileage LOW, cars_small.registration HIGHDATE";

            SQLCommon common = new SQLCommon();
            common.SkylineType = SQLCommon.Algorithm.BNLSort;
            string sqlBNL = common.parsePreferenceSQL(strPrefSQL);

            int amountOfTupelsBNL = 0;

            SqlConnection cnnSQL = new SqlConnection(strConnection);
            cnnSQL.InfoMessage += cnnSQL_InfoMessage;
            try
            {
                cnnSQL.Open();

                 //BNL
                SqlCommand sqlCommand = new SqlCommand(sqlBNL, cnnSQL);
                SqlDataReader  sqlReader = sqlCommand.ExecuteReader();

                if (sqlReader.HasRows)
                {
                    while (sqlReader.Read())
                    {
                        amountOfTupelsBNL++;
                    }
                }
                cnnSQL.Close();
            }
            catch (Exception ex)
            {
                Assert.Fail("Connection failed:" + ex.Message);
            }

            Assert.AreEqual(24, amountOfTupelsBNL, 0, "Amount of tupels does not match");
        }


        [TestMethod]
        public void TestSKYLINEAmountsOfTupels11Equal()
        {

            string strPrefSQL = "SELECT cars_small.price,cars_small.mileage,cars_small.horsepower,cars_small.enginesize,cars_small.consumption,cars_small.doors,colors.name,fuels.name,bodies.name,cars_small.title,makes.name,conditions.name FROM cars_small LEFT OUTER JOIN colors ON cars_small.color_id = colors.ID LEFT OUTER JOIN fuels ON cars_small.fuel_id = fuels.ID LEFT OUTER JOIN bodies ON cars_small.body_id = bodies.ID LEFT OUTER JOIN makes ON cars_small.make_id = makes.ID LEFT OUTER JOIN conditions ON cars_small.condition_id = conditions.ID " +
                "SKYLINE OF cars_small.price LOW, cars_small.mileage LOW, cars_small.horsepower HIGH, cars_small.enginesize HIGH, cars_small.consumption LOW, cars_small.doors HIGH " +
                ", colors.name ('rot' == 'blau' >> OTHERS EQUAL >> 'grau')  , fuels.name ('Benzin' >> OTHERS EQUAL >> 'Diesel') , bodies.name ('Kleinwagen' >> 'Bus' >> 'Kombi' >> 'Roller' >> OTHERS EQUAL >> 'Pick-Up') " +
                ", cars_small.title ('MERCEDES-BENZ SL 600' >> OTHERS EQUAL) , makes.name ('ASTON MARTIN' >> 'VW' == 'Audi' >> OTHERS EQUAL >> 'FERRARI') , conditions.name ('Neu' >> OTHERS EQUAL)";

            SQLCommon common = new SQLCommon();
            common.SkylineType = SQLCommon.Algorithm.NativeSQL;
            string sqlNative = common.parsePreferenceSQL(strPrefSQL);
            common.SkylineType = SQLCommon.Algorithm.BNLSort;
            string sqlBNL = common.parsePreferenceSQL(strPrefSQL);


            int amountOfTupelsNative = 0;
            int amountOfTupelsBNL = 0;

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
                        amountOfTupelsNative++;
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


                cnnSQL.Close();
            }
            catch (Exception ex)
            {
                Assert.Fail("Connection failed:" + ex.Message);
            }

            Assert.AreEqual(amountOfTupelsNative, amountOfTupelsBNL, 0, "Amount of tupels does not match");
        }



        [TestMethod]
        public void TestSKYLINEAmountsOfTupels11EqualWithHexagon()
        {

            string strPrefSQL = "SELECT cars_small.price,cars_small.mileage,cars_small.horsepower,cars_small.enginesize,cars_small.consumption,cars_small.doors,colors.name,fuels.name,bodies.name,cars_small.title,makes.name,conditions.name FROM cars_small LEFT OUTER JOIN colors ON cars_small.color_id = colors.ID LEFT OUTER JOIN fuels ON cars_small.fuel_id = fuels.ID LEFT OUTER JOIN bodies ON cars_small.body_id = bodies.ID LEFT OUTER JOIN makes ON cars_small.make_id = makes.ID LEFT OUTER JOIN conditions ON cars_small.condition_id = conditions.ID " +
                "SKYLINE OF cars_small.price LOW 3000 EQUAL, cars_small.mileage LOW 20000 EQUAL, cars_small.horsepower HIGH 20 EQUAL, cars_small.enginesize HIGH 1000 EQUAL, cars_small.consumption LOW 15 EQUAL, cars_small.doors HIGH ";

            SQLCommon common = new SQLCommon();
            common.SkylineType = SQLCommon.Algorithm.NativeSQL;
            string sqlNative = common.parsePreferenceSQL(strPrefSQL);
            common.SkylineType = SQLCommon.Algorithm.BNLSort;
            string sqlBNL = common.parsePreferenceSQL(strPrefSQL);
            common.SkylineType = SQLCommon.Algorithm.Hexagon;
            string sqlHexagon = common.parsePreferenceSQL(strPrefSQL);


            int amountOfTupelsNative = 0;
            int amountOfTupelsBNL = 0;
            int amountOfTupelsHexagon = 0;

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
                        amountOfTupelsNative++;
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

                cnnSQL.Close();
            }
            catch (Exception ex)
            {
                Assert.Fail("Connection failed:" + ex.Message);
            }

            Assert.AreEqual(amountOfTupelsNative, amountOfTupelsBNL, 0, "Amount of tupels does not match");
        }

        



        [TestMethod]
        public void TestSKYLINEAmountsOfTupels2Incomparable()
        {

            string strPrefSQL = "SELECT cars_small.price,colors.name FROM cars_small "+
            "LEFT OUTER JOIN colors ON cars_small.color_id = colors.ID " +
            "SKYLINE OF cars_small.price LOW, colors.name ('rot' == 'pink' >> OTHERS INCOMPARABLE >> 'grau') ";

            SQLCommon common = new SQLCommon();
            common.SkylineType = SQLCommon.Algorithm.NativeSQL;
            string sqlNative = common.parsePreferenceSQL(strPrefSQL);
            common.SkylineType = SQLCommon.Algorithm.BNLSort;
            string sqlBNL = common.parsePreferenceSQL(strPrefSQL);

            int amountOfTupelsNative = 0;
            int amountOfTupelsBNL = 0;

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
                        amountOfTupelsNative++;
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
                cnnSQL.Close();
            }
            catch (Exception ex)
            {
                Assert.Fail("Connection failed:" + ex.Message);
            }

            Assert.AreEqual(amountOfTupelsNative, amountOfTupelsBNL, 0, "Amount of tupels does not match");
        }


        [TestMethod]
        public void TestSKYLINEAmountsOfTupels3()
        {
            string strPrefSQL = "SELECT * FROM cars SKYLINE OF cars.price LOW, cars.mileage LOW, cars.horsepower HIGH";

            SQLCommon common = new SQLCommon();
            common.SkylineType = SQLCommon.Algorithm.NativeSQL;
            string sqlNative = common.parsePreferenceSQL(strPrefSQL);
            common.SkylineType = SQLCommon.Algorithm.BNLSort;
            string sqlBNL = common.parsePreferenceSQL(strPrefSQL);

            int amountOfTupelsNative = 0;
            int amountOfTupelsBNL = 0;


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
                        amountOfTupelsNative++;
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
                cnnSQL.Close();
            }
            catch (Exception ex)
            {
                Assert.Fail("Connection failed:" + ex.Message);
            }

            Assert.AreEqual(amountOfTupelsNative, amountOfTupelsBNL, 0, "Amount of tupels does not match");

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
        public void TestSKYLINEDisfavourGruen()
        {
            string strPrefSQL = "SELECT cars.id, cars.title, cars.Price, colors.Name FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID SKYLINE OF cars.price LOW, colors.name DISFAVOUR 'grün'";

            string expected = "SELECT cars.id, cars.title, cars.Price, colors.Name FROM cars LEFT OUTER JOIN colors ON cars.color_id = colors.ID WHERE NOT EXISTS(SELECT cars_INNER.id, cars_INNER.title, cars_INNER.Price, colors_INNER.Name FROM cars cars_INNER LEFT OUTER JOIN colors colors_INNER ON cars_INNER.color_id = colors_INNER.ID WHERE cars_INNER.price <= cars.price AND CASE WHEN colors_INNER.name = 'grün' THEN 1 ELSE 2 END >= CASE WHEN colors.name = 'grün' THEN 1 ELSE 2 END AND ( cars_INNER.price < cars.price OR CASE WHEN colors_INNER.name = 'grün' THEN 1 ELSE 2 END > CASE WHEN colors.name = 'grün' THEN 1 ELSE 2 END) )";
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

            string expected = "SELECT * FROM cars WHERE NOT EXISTS(SELECT * FROM cars cars_INNER WHERE cars_INNER.price <= cars.price AND cars_INNER.mileage <= cars.mileage AND cars_INNER.horsepower >= cars.horsepower AND ( cars_INNER.price < cars.price OR cars_INNER.mileage < cars.mileage OR cars_INNER.horsepower > cars.horsepower) ) ORDER BY price ASC, mileage ASC, horsepower DESC";
            SQLCommon common = new SQLCommon();
            string actual = common.parsePreferenceSQL(strPrefSQL);

            // assert

            Assert.AreEqual(expected.Trim(), actual.Trim(), true, "SQL not built correctly");



        }


        [TestMethod]
        public void TestSKYLINE6Dimensions()
        {
            string strPrefSQL = "SELECT * FROM cars SKYLINE OF cars.price LOW, cars.mileage LOW, cars.horsepower HIGH, cars.enginesize HIGH, cars.registration HIGHDATE, cars.consumption LOW ORDER BY price ASC, mileage ASC, horsepower DESC, enginesize DESC, registration DESC, consumption ASC";

            string expected = "SELECT * FROM cars WHERE NOT EXISTS(SELECT * FROM cars cars_INNER WHERE cars_INNER.price <= cars.price AND cars_INNER.mileage <= cars.mileage AND cars_INNER.horsepower >= cars.horsepower AND cars_INNER.enginesize >= cars.enginesize AND DATEDIFF(minute, '1900-01-01', cars_INNER.registration)  >= DATEDIFF(minute, '1900-01-01', cars.registration)  AND cars_INNER.consumption <= cars.consumption AND ( cars_INNER.price < cars.price OR cars_INNER.mileage < cars.mileage OR cars_INNER.horsepower > cars.horsepower OR cars_INNER.enginesize > cars.enginesize OR DATEDIFF(minute, '1900-01-01', cars_INNER.registration)  > DATEDIFF(minute, '1900-01-01', cars.registration)  OR cars_INNER.consumption < cars.consumption) ) ORDER BY price ASC, mileage ASC, horsepower DESC, enginesize DESC, registration DESC, consumption ASC";
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
