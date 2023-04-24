using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration.Install;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace CVUploadService
{
    [RunInstaller(true)]
    public partial class Installer1 : System.Configuration.Install.Installer
    {
        public Installer1()
        {
            InitializeComponent();
        }

        public override void Install(IDictionary stateSaver)
        {
            base.Install(stateSaver);
        }

        protected override void OnAfterInstall(IDictionary savedState)
        {
            base.OnAfterInstall(savedState);
            ModifyConnectionString();
        }

        void ModifyConnectionString()
        {

            string DataSource = @Context.Parameters["DataSource"].Replace("\\\\", "\\"),
                InitialCatalog = @Context.Parameters["InitialCatalog"].Replace("\\\\", "\\"),
                UserID = @Context.Parameters["UserID"].Replace("\\\\", "\\"),
                Password = @Context.Parameters["Password"].Replace("\\\\", "\\"),
                TargetDir = Context.Parameters["TargetDir"];


            string ArmConnection = "Data Source=;Initial Catalog=;User ID=;Password=;";
            string ArmConnectionNew = "Data Source=" + DataSource + ";Initial Catalog=" + InitialCatalog + ";User ID=" + UserID + ";Password=" + Password + ";Pooling=True;Max Pool Size=500;MultipleActiveResultSets=True;Application Name=EntityFramework;Connection Timeout=1800";


            string fname = Context.Parameters["TargetDir"] + @"CVUploadService.exe.config";
            StreamReader reader = new StreamReader(fname);
            string input = reader.ReadToEnd();
            reader.Close();
            using (StreamWriter writer = new StreamWriter(fname, false))
            {
                {

                    input = input.Replace(ArmConnection, ArmConnectionNew);
                    string output = input;
                    writer.Write(output);
                }
                writer.Close();
            }

        }
    }
}
