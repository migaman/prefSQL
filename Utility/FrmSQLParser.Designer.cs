namespace Utility
{
    partial class FrmSQLParser
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.groupBox1 = new System.Windows.Forms.GroupBox();
            this.optBNL = new System.Windows.Forms.RadioButton();
            this.optSQL = new System.Windows.Forms.RadioButton();
            this.checkBox1 = new System.Windows.Forms.CheckBox();
            this.btnExecute = new System.Windows.Forms.Button();
            this.txtPrefSQL = new System.Windows.Forms.TextBox();
            this.gridSkyline = new System.Windows.Forms.DataGridView();
            this.optHexagon = new System.Windows.Forms.RadioButton();
            this.label1 = new System.Windows.Forms.Label();
            this.txtTime = new System.Windows.Forms.TextBox();
            this.groupBox1.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.gridSkyline)).BeginInit();
            this.SuspendLayout();
            // 
            // groupBox1
            // 
            this.groupBox1.Controls.Add(this.optHexagon);
            this.groupBox1.Controls.Add(this.optBNL);
            this.groupBox1.Controls.Add(this.optSQL);
            this.groupBox1.Location = new System.Drawing.Point(12, 12);
            this.groupBox1.Name = "groupBox1";
            this.groupBox1.Size = new System.Drawing.Size(322, 142);
            this.groupBox1.TabIndex = 1;
            this.groupBox1.TabStop = false;
            this.groupBox1.Text = "Skyline Algorithm";
            // 
            // optBNL
            // 
            this.optBNL.AutoSize = true;
            this.optBNL.Location = new System.Drawing.Point(54, 76);
            this.optBNL.Name = "optBNL";
            this.optBNL.Size = new System.Drawing.Size(46, 17);
            this.optBNL.TabIndex = 2;
            this.optBNL.TabStop = true;
            this.optBNL.Text = "BNL";
            this.optBNL.UseVisualStyleBackColor = true;
            // 
            // optSQL
            // 
            this.optSQL.AutoSize = true;
            this.optSQL.Location = new System.Drawing.Point(54, 44);
            this.optSQL.Name = "optSQL";
            this.optSQL.Size = new System.Drawing.Size(46, 17);
            this.optSQL.TabIndex = 1;
            this.optSQL.TabStop = true;
            this.optSQL.Text = "SQL";
            this.optSQL.UseVisualStyleBackColor = true;
            // 
            // checkBox1
            // 
            this.checkBox1.AutoSize = true;
            this.checkBox1.Location = new System.Drawing.Point(387, 23);
            this.checkBox1.Name = "checkBox1";
            this.checkBox1.Size = new System.Drawing.Size(137, 17);
            this.checkBox1.TabIndex = 3;
            this.checkBox1.Text = "Show Skyline Attributes";
            this.checkBox1.UseVisualStyleBackColor = true;
            // 
            // btnExecute
            // 
            this.btnExecute.Location = new System.Drawing.Point(339, 254);
            this.btnExecute.Name = "btnExecute";
            this.btnExecute.Size = new System.Drawing.Size(129, 28);
            this.btnExecute.TabIndex = 4;
            this.btnExecute.Text = "Get Query!";
            this.btnExecute.UseVisualStyleBackColor = true;
            this.btnExecute.Click += new System.EventHandler(this.btnExecute_Click);
            // 
            // txtPrefSQL
            // 
            this.txtPrefSQL.Location = new System.Drawing.Point(12, 172);
            this.txtPrefSQL.Multiline = true;
            this.txtPrefSQL.Name = "txtPrefSQL";
            this.txtPrefSQL.Size = new System.Drawing.Size(804, 55);
            this.txtPrefSQL.TabIndex = 5;
            // 
            // gridSkyline
            // 
            this.gridSkyline.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            this.gridSkyline.Location = new System.Drawing.Point(12, 338);
            this.gridSkyline.Name = "gridSkyline";
            this.gridSkyline.Size = new System.Drawing.Size(804, 217);
            this.gridSkyline.TabIndex = 7;
            // 
            // optHexagon
            // 
            this.optHexagon.AutoSize = true;
            this.optHexagon.Location = new System.Drawing.Point(54, 109);
            this.optHexagon.Name = "optHexagon";
            this.optHexagon.Size = new System.Drawing.Size(68, 17);
            this.optHexagon.TabIndex = 3;
            this.optHexagon.TabStop = true;
            this.optHexagon.Text = "Hexagon";
            this.optHexagon.UseVisualStyleBackColor = true;
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(396, 88);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(66, 13);
            this.label1.TabIndex = 8;
            this.label1.Text = "elapsed time";
            // 
            // txtTime
            // 
            this.txtTime.Location = new System.Drawing.Point(508, 87);
            this.txtTime.Name = "txtTime";
            this.txtTime.Size = new System.Drawing.Size(100, 20);
            this.txtTime.TabIndex = 9;
            // 
            // FrmSQLParser
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(952, 618);
            this.Controls.Add(this.txtTime);
            this.Controls.Add(this.label1);
            this.Controls.Add(this.gridSkyline);
            this.Controls.Add(this.txtPrefSQL);
            this.Controls.Add(this.btnExecute);
            this.Controls.Add(this.checkBox1);
            this.Controls.Add(this.groupBox1);
            this.Name = "FrmSQLParser";
            this.Text = "FrmSQLParser";
            this.Load += new System.EventHandler(this.FrmSQLParser_Load);
            this.groupBox1.ResumeLayout(false);
            this.groupBox1.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)(this.gridSkyline)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.GroupBox groupBox1;
        private System.Windows.Forms.RadioButton optBNL;
        private System.Windows.Forms.RadioButton optSQL;
        private System.Windows.Forms.CheckBox checkBox1;
        private System.Windows.Forms.Button btnExecute;
        private System.Windows.Forms.TextBox txtPrefSQL;
        private System.Windows.Forms.DataGridView gridSkyline;
        private System.Windows.Forms.RadioButton optHexagon;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.TextBox txtTime;
    }
}