using System;
using System.Data;
using System.Diagnostics;
using System.Windows.Forms;
using prefSQL.SQLParser;
using prefSQL.SQLSkyline;

namespace Utility
{
    public partial class FrmSQLParser : Form
    {
        public FrmSQLParser()
        {
            InitializeComponent();
        }

        private void FrmSQLParser_Load(object sender, EventArgs e)
        {
            optSQL.Checked = true;
            string strSQL = "SELECT t1.id, t1.title, t1.price, t1.mileage, t1.enginesize FROM cars t1 " +
                "LEFT OUTER JOIN colors ON t1.color_id = colors.ID SKYLINE OF t1.price LOW, t1.mileage LOW";
            txtPrefSQL.Text = strSQL;

 
        }


        private void btnExecute_Click(object sender, EventArgs e)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            btnExecute.Enabled = false;
            
            SQLCommon parser = new SQLCommon();
            if(optSQL.Checked)
            {
                parser.SkylineType = new SkylineSQL();
            }
            else if (optBNL.Checked)
            {
                parser.SkylineType = new SkylineBNLSort();
            }
            else if (optHexagon.Checked)
            {
                parser.SkylineType = new SkylineHexagon();
            }
            else if (optDQ.Checked)
            {
                parser.SkylineType = new SkylineDQ();
            }

            if (chkShowSkyline.Checked)
            {
                parser.ShowSkylineAttributes = true;
            }
            else
            {
                parser.ShowSkylineAttributes = false;
            }

            DataTable dt = parser.ParseAndExecutePrefSQL(Helper.ConnectionString, Helper.ProviderName, txtPrefSQL.Text);           

            BindingSource sBind = new BindingSource();
            sBind.DataSource = dt;
            
            gridSkyline.AutoGenerateColumns = true;
            gridSkyline.DataSource = dt;

            gridSkyline.DataSource = sBind;
            gridSkyline.Refresh();

            sw.Stop();
            
            txtTime.Text = sw.ElapsedMilliseconds.ToString();
            txtTimeAlgo.Text = parser.TimeInMilliseconds.ToString();
            txtRecords.Text = dt.Rows.Count.ToString();


            btnExecute.Enabled = true;
        }

    }
}
