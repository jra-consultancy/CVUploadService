namespace CVUploadService
{
    partial class ProjectInstaller
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

        #region Component Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.CVUploadProcess = new System.ServiceProcess.ServiceProcessInstaller();
            this.CVUpload = new System.ServiceProcess.ServiceInstaller();
            // 
            // CVUploadProcess
            // 
            this.CVUploadProcess.Account = System.ServiceProcess.ServiceAccount.LocalService;
            this.CVUploadProcess.Password = null;
            this.CVUploadProcess.Username = null;
            // 
            // CVUpload
            // 
            this.CVUpload.DelayedAutoStart = true;
            this.CVUpload.ServiceName = "CV Upload Service";
            // 
            // ProjectInstaller
            // 
            this.Installers.AddRange(new System.Configuration.Install.Installer[] {
            this.CVUploadProcess,
            this.CVUpload});
            this.AfterInstall += new System.Configuration.Install.InstallEventHandler(this.ProjectInstaller_AfterInstall);

        }

        #endregion

        private System.ServiceProcess.ServiceProcessInstaller CVUploadProcess;
        private System.ServiceProcess.ServiceInstaller CVUpload;
    }
}