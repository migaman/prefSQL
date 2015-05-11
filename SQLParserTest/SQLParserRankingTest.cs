namespace prefSQL.SQLParserTest
{
    class SQLParserRankingTest
    {

        private string[] GetPreferences()
        {
            string[] strPrefSQL = new string[13];

            //1 numerical preference
            strPrefSQL[0] = "SELECT t1.id AS ID, t1.title, t1.price FROM cars_small t1 RANKING OF t1.price LOW 1";
            //3 numerical preferences
            strPrefSQL[1] = "SELECT * FROM cars_small t1 RANKING OF t1.price LOW 0.5, t1.mileage LOW 0.3, t1.horsepower HIGH 0.2";
            //6 numerical preferences with EQUAL STEPS
            strPrefSQL[2] = "SELECT cars_small.price,cars_small.mileage,cars_small.horsepower,cars_small.enginesize,cars_small.consumption,cars_small.doors,colors.name,fuels.name,bodies.name,cars_small.title,makes.name,conditions.name FROM cars_small LEFT OUTER JOIN colors ON cars_small.color_id = colors.ID LEFT OUTER JOIN fuels ON cars_small.fuel_id = fuels.ID LEFT OUTER JOIN bodies ON cars_small.body_id = bodies.ID LEFT OUTER JOIN makes ON cars_small.make_id = makes.ID LEFT OUTER JOIN conditions ON cars_small.condition_id = conditions.ID " +
                "RANKING OF cars_small.price LOW 3000 EQUAL, cars_small.mileage LOW 20000 EQUAL, cars_small.horsepower HIGH 20 EQUAL, cars_small.enginesize HIGH 1000 EQUAL, cars_small.consumption LOW 15 EQUAL, cars_small.doors HIGH ";


            //Preference with TOP Keyword
            //3 numerical preferences with TOP Keyword
            strPrefSQL[3] = "SELECT TOP 5 t1.title FROM cars_small t1 RANKING OF t1.price LOW, t1.mileage LOW, t1.horsepower HIGH";

            //OTHERS EQUAL at the end
            strPrefSQL[4] = "SELECT t1.id, t1.title AS AutoTitel, t1.price, t1.mileage, colors.name FROM cars_small t1 LEFT OUTER JOIN colors ON t1.color_id = colors.ID RANKING OF t1.price LOW, colors.name ('red' >> 'blue' >> OTHERS EQUAL)";
            //OTHERS EQUAL at the beginning
            strPrefSQL[5] = "SELECT t1.id, t1.title, t1.price, t1.mileage, colors.name FROM cars_small t1 LEFT OUTER JOIN colors ON t1.color_id = colors.ID RANKING OF t1.price LOW, colors.name (OTHERS EQUAL >> 'blue')";
            //OTHERS EQUAL in the middle
            strPrefSQL[6] = "SELECT t1.id, t1.title, t1.price, t1.mileage, colors.name FROM cars_small t1 LEFT OUTER JOIN colors ON t1.color_id = colors.ID RANKING OF t1.price LOW, colors.name ('red' >> OTHERS EQUAL >> 'blue')";

            return strPrefSQL;
        }


        /**
         * This test checks if the algorithms return the same amount of tupels for different prefSQL statements
         * At the moment it contains the BNL, nativeSQL and Hexagon algorithm. It is intendend to add the D&Q as soon
         * as it works
         * 
         * */
        /*[TestMethod]
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
                if (i < 6)
                {
                    common.SkylineType = new SkylineDQ();
                    dtDQ = common.parseAndExecutePrefSQL(strConnection, driver, strPrefSQL[i]);
                }


                //Check tuples (every algorithm should deliver the same amount of tuples)
                Assert.AreEqual(dtNative.Rows.Count, dtBNL.Rows.Count, 0, "BNL Amount of tupels in query " + i + " do not match");
                Assert.AreEqual(dtNative.Rows.Count, dtBNLSort.Rows.Count, 0, "BNLSort Amount of tupels in query " + i + " do not match");
                Assert.AreEqual(dtNative.Rows.Count, dtHexagon.Rows.Count, 0, "Hexagon Amount of tupels in query " + i + " do not match");
                //D&Q does not work with incomparable tuples
                if (i < 6)
                {
                    Assert.AreEqual(dtNative.Rows.Count, dtDQ.Rows.Count, 0, "D&Q Amount of tupels in query " + i + " do not match");
                }

            }
        }*/
    }
}
