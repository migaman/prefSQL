using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;
using System.Collections;
using System.Collections.Generic;



namespace prefSQL.SQLSkyline
{

    public class SP_SkylineHexagon
    {
        //Only this parameters are different beteen SQL CLR function and Utility class
        private const string connectionstring = "Data Source=localhost;Initial Catalog=eCommerce;Integrated Security=True";
        private const int MaxSize = 4000;


        [Microsoft.SqlServer.Server.SqlProcedure]
        public static void getSkylineHexagon(SqlString strQuery, SqlString strOperators, SqlString strQueryConstruction, SqlBoolean isDebug)
        {
            ArrayList[] btg = null;
            int[] next = null;
            int[] prev = null;
            int[] level = null;
            int[] weight = null;


            String strSQL = strQuery.ToString();
            ArrayList resultCollection = new ArrayList();
            ArrayList resultstringCollection = new ArrayList();
            string[] operators = strOperators.ToString().Split(';');
            int amountOfPreferences = operators.GetUpperBound(0) + 1;
            construction(amountOfPreferences, strQueryConstruction.ToString(), ref btg, ref next, ref prev, ref level, ref weight);
            int startSkylineColumns = 0;

            SqlConnection connection = new SqlConnection(connectionstring);
            try
            {
                //Some checks
                if (strSQL.Length == MaxSize)
                {
                    throw new Exception("Query is too long. Maximum size is " + MaxSize);
                }
                connection.Open();

                SqlDataAdapter dap = new SqlDataAdapter(strSQL, connection);
                DataTable dt = new DataTable();
                dap.Fill(dt);

                //Read start of skyline
                int i = 0;
                foreach (DataColumn col in dt.Columns)
                {

                    if (col.Caption.StartsWith("Rank"))
                    {
                        startSkylineColumns = i;
                        break;
                    }
                    i++;

                }


                DataTableReader sqlReader = dt.CreateDataReader();


                //Read all records only once. (SqlDataReader works forward only!!)
                while (sqlReader.Read())
                {
                    add(sqlReader, amountOfPreferences, startSkylineColumns, ref btg, ref weight);
                }
                sqlReader.Close();

                findBMO(amountOfPreferences, ref btg, ref next, ref prev, ref level, ref weight);


                //Now read next list
                int iItem = 0;

                //Unitl no more nodes are found
                while (iItem != -1)
                {
                    //Add all records of this node
                    if (btg[iItem] != null)
                    {
                        foreach (long[] test in btg[iItem])
                        {
                            resultCollection.Add(test);
                        }
                    }

                    //Goto the next node
                    iItem = next[iItem];

                }

                if (isDebug == true)
                {
                    System.Diagnostics.Debug.WriteLine(resultCollection.Count);
                }



            }
            catch (Exception ex)
            {
                //Pack Errormessage in a SQL and return the result
                string strError = "SELECT 'Fehler in SP_SkylineHexagon: ";
                strError += ex.Message.Replace("'", "''");
                strError += "'";

                if (isDebug == true)
                {
                    System.Diagnostics.Debug.WriteLine(strError);

                }
                else
                {
                    SqlContext.Pipe.Send(strError);
                }


            }
            finally
            {
                if (connection != null)
                    connection.Close();
            }

        }

        private static void remove(int id, int index, ref ArrayList[] btg, ref int[] next, ref int[] prev, ref int[] level, ref int[] weight)
        {
            //check if the node has already been removed
            if (prev[id] == -1)
            {
                return;
            }
            if (next[prev[id]] != id)
            {
                return;
            }
            else
            {
                //remove the node from next/prev relation
                next[prev[id]] = next[id];
                if (next[id] != -1)
                {
                    prev[next[id]] = prev[id];
                }
                //else
                /*{
                    prev[id] = -1;
                }*/
                //prev[id] = prev[id];

                //next[id] = -1;
                //prev[id] = -1;

                //remove tuples in node
                btg[id] = null;

            }

            int i = 1;

            //remove followers
            for (i = 0; i <= index; i++)
            {
                // follow the edge for preference i (only if not already on last level)
                if (id + weight[i] <= level.GetUpperBound(0) && level[id + weight[i]] == level[id] + 1)
                {
                    remove(id + weight[i], i, ref btg, ref next, ref prev, ref level, ref weight);
                }
            }



        }

        //Find BestMatchesOnly
        private static void findBMO(int amountPreferences, ref ArrayList[] btg, ref int[] next, ref int[] prev, ref int[] level, ref int[] weight)
        {
            int last = 0;

            // special case: top node is not empty
            if (btg[0] != null)
            {
                //Top node contains a tuple --> This is the perfect tuple. Set next to -1,
                next[0] = -1;

            }
            else
            {
                //follow the breadth-first walk
                int cur = 1;
                //while (cur != -1)
                while (cur != -1 && next[cur] != -1) //<= btg.GetUpperBound(0)
                {
                    if (btg[cur] != null)
                    {
                        // non-empty node belonging to BMO set
                        int nextCur = next[cur];
                        last = cur;
                        // remove all nodes dominated by current
                        for (int i = 0; i < amountPreferences; i++)
                        {
                            // check if there is an edge for Pi
                            if (cur + weight[i] <= level.GetUpperBound(0))
                            {
                                if (level[cur + weight[i]] == level[cur] + 1)
                                {
                                    remove(cur + weight[i], i, ref btg, ref next, ref prev, ref level, ref weight);
                                }
                            }


                        }
                        cur = nextCur;

                    }
                    else
                    {
                        // node is empty: remove from next/prev
                        int nextCur = next[cur];

                        next[last] = next[cur];
                        prev[next[cur]] = last;
                        next[cur] = -1; //gibt es nicht mehr dieses node
                        prev[cur] = -1;
                        /*next[last] = cur;
                        if (next[cur] != -1)
                        {
                            prev[next[cur]] = last;
                        }*/

                        cur = nextCur; //Damit Breadt-First Walk
                    }

                    //Goto next node
                    //cur++;
                }

            }



        }
        private static void add(DataTableReader sqlReader, int amountOfPreferences, int startSkylineColumns, ref ArrayList[] btg, ref int[] weight) //add tuple
        {
            //create int array from sqlReader
            long[] tuple = new long[sqlReader.FieldCount - startSkylineColumns];
            for (int iCol = startSkylineColumns; iCol < sqlReader.FieldCount; iCol++)
            {
                tuple[iCol - startSkylineColumns] = sqlReader.GetInt64(iCol);
            }



            //1: procedure add(tuple)
            // compute the node ID for the tuple
            long id = 0;
            for (int i = 0; i < amountOfPreferences; i++)
            {
                //id = id + levelPi(tuple) * weight(i);
                id = id + tuple[i] * weight[i];
            }

            // add tuple to its node
            if (btg[id] == null)
            {
                btg[id] = new ArrayList();
            }
            btg[id].Add(tuple);


        }


        private static void construction(int amountOfPreferences, string strQuery, ref ArrayList[] btg, ref int[] next, ref int[] prev, ref int[] level, ref int[] weight)
        {

            SqlConnection connection = new SqlConnection(connectionstring);
            try
            {
                connection.Open();



                SqlDataAdapter dap = new SqlDataAdapter(strQuery, connection);
                DataTable dt = new DataTable();
                dap.Fill(dt);



                int[] maxPreferenceLevel = new int[amountOfPreferences];
                for (int i = 0; i < amountOfPreferences; i++)
                {
                    maxPreferenceLevel[i] = int.Parse(dt.Rows[0][i].ToString());
                }

                //calculate edge weights
                weight = new int[amountOfPreferences];
                weight[amountOfPreferences - 1] = 1;

                for (int i = amountOfPreferences - 2; i >= 0; i--)
                {
                    weight[i] = weight[i + 1] * (maxPreferenceLevel[i + 1] + 1);
                }


                // calculate the BTG size
                int sizeNodes = 1;
                for (int i = 0; i < amountOfPreferences; i++)
                {
                    sizeNodes *= (maxPreferenceLevel[i] + 1);
                }


                //ArrayList listOfTuples 
                btg = new ArrayList[sizeNodes];
                next = new int[sizeNodes];
                prev = new int[sizeNodes];
                level = new int[sizeNodes];

                int workSize = 1;
                for (int i = 0; i < amountOfPreferences; i++)
                {
                    workSize += maxPreferenceLevel[i];
                }


                // arrays needed for init computation

                // stores highest ID found for each level by now
                int[] work = new int[workSize]; //int[max(P) + 1]

                // stores first ID found for each level by now
                int[] first = new int[workSize]; //int[max(P) + 1]

                // initialize the arrays
                next[0] = 1;

                //loop over the node IDs
                for (int id = 1; id <= sizeNodes - 1; id++)
                {
                    // compute level of the node (with help of weights)
                    int curLvl = 0;
                    int tmpID = id; //wrong code in paper --> use of id inside de id-loop
                    for (int i = 0; i <= amountOfPreferences - 1; i++)
                    {
                        double dblLevel = (double)tmpID / weight[i];
                        curLvl = curLvl + (int)Math.Floor(dblLevel);
                        tmpID = tmpID - ((int)Math.Floor((dblLevel)) * weight[i]);
                    }

                    if (first[curLvl] == 0)
                    {
                        first[curLvl] = id;
                    }
                    // set next node in the level
                    //the current node is n’s next node and n is the current node’s previous node
                    next[work[curLvl]] = id;
                    prev[id] = work[curLvl];
                    work[curLvl] = id; //wrong code in paper

                    //For each ID, we determine to which level the corresponding node belongs
                    level[id] = curLvl;
                }

                // init next relation for last nodes in the levels
                for (int curLvl = 0; curLvl < workSize - 1; curLvl++)
                {
                    next[work[curLvl]] = first[curLvl + 1];

                    //
                    prev[first[curLvl + 1]] = work[curLvl];
                }
                /*for (int curLvl = 1; curLvl < workSize; curLvl++)
                {
                    prev[first[curLvl]] = work[curLvl - 1];
                }*/

                //set next node of bottom node
                next[sizeNodes - 1] = -1;
                //set previous node of top node
                prev[0] = -1;



            }
            catch (Exception e)
            {
                System.Diagnostics.Debug.WriteLine(e.Message);
            }
        }


    }
}
