

///// WEB.CONFIG /////
  <appSettings>
    <add key="CommandTimeOut" value="120"/>
    <add key="CaminhoArquivo" value="\\\\FileServer\\Desenvolvimento\\Query\\"/>
    <add key ="Users" value="User01,User02,User03"/>
  </appSettings>


///// MÉTODO DE CRIAÇÃO DE TABELA COM MESMA ESTRUTURA DO DATATABLE CRIADO PELO SELECT /////
        public static string CreateTABLE(string tableName, DataTable table)                                                                          
        {                                                                                                                                            
            string sqlsc;                                                                                                                            
            sqlsc = "CREATE TABLE " + tableName + "(";                                                                                               
            for (int i = 0; i < table.Columns.Count; i++)                                                                                            
            {                                                                                                                                        
                sqlsc += "\n [" + table.Columns[i].ColumnName + "] ";                                                                                
                string columnType = table.Columns[i].DataType.ToString();                                                                            
                switch (columnType)                                                                                                                  
                {                                                                                                                                    
                    case "System.Int32":                                                                                                             
                        sqlsc += " int ";                                                                                                            
                        break;                                                                                                                       
                    case "System.Int64":                                                                                                             
                        sqlsc += " bigint ";                                                                                                         
                        break;                                                                                                                       
                    case "System.Int16":                                                                                                             
                        sqlsc += " smallint";                                                                                                        
                        break;                                                                                                                       
                    case "System.Byte":                                                                                                              
                        sqlsc += " tinyint";  
                        break;  
                    case "System.Decimal":   
                        sqlsc += " float ";     
                        break;     
                    case "System.DateTime":   
                        sqlsc += " datetime ";  
                        break;       
                    case "System.String":      
                    default:  
                        sqlsc += string.Format(" nvarchar({0}) ", table.Columns[i].MaxLength == -1 ? "max" : table.Columns[i].MaxLength.ToString()); 
                        break;  
                }      
                sqlsc += ",";    
            }    
            return sqlsc.Substring(0, sqlsc.Length - 1) + "\n)";      
        }     


///// GRAVAR LOGS DE EXECUÇÕES /////
    public class Logs
    {
        public void GravaLogSQL(string userName, double stopwatch, string comandoGravaLog)   
        {                                                                                                                                   
                SqlConnection connectionGravaLog = new SqlConnection(ConfigurationManager.ConnectionStrings["AdminSQL"].ConnectionString);  
                connectionGravaLog.Open();       
                SqlCommand commandGravaLog = connectionGravaLog.CreateCommand();                                                            
                commandGravaLog.CommandText = comandoGravaLog;                                                                              
                SqlDataReader readerGravaLog = commandGravaLog.ExecuteReader();                                                             
                connectionGravaLog.Close();    
                readerGravaLog.Close();    
        }      
    }


///// EVENTO DO BOTÃO DE EXECUÇÃO DA QUERY /////
protected void Button1_Click(object sender, EventArgs e) {
            String nomeTabelaDestino = "ExecQuery_" + DropDownList_DataSource.Text + "_" + DateTime.Now.ToString("yyyy_MM_dd_HHmmss");
            string userName = this.User.Identity.Name.ToString(); //System.Security.Principal.WindowsIdentity.GetCurrent().Name;
            string AllowedUsers = ConfigurationManager.AppSettings["Users"].ToString();
            string[] AllowedUsersArray = AllowedUsers.Split(',');
            double stopwatch = 0;

            if (AllowedUsersArray.ToList().ToString().Contains(userName.ToString()) == false)
                if (AllowedUsersArray.Contains(userName.ToString()) == false) {
                    Response.Redirect("AcessoNegado.aspx");
                }

 ///// BUSCA E GRAVAÇÃO DE DADOS DO POSTGRESQL /////
            if (DropDownList_DataSource.SelectedValue == "Postgres01" || DropDownList_DataSource.SelectedValue == " Postgres02") {    
             NpgsqlConnection connPg = new NpgsqlConnection(ConfigurationManager.ConnectionStrings[DropDownList_DataSource.SelectedValue].ConnectionString);  
                connPg.Open();    
                try { 
                    NpgsqlCommand commandEmp001 = new NpgsqlCommand(TextBox_Comando.Text, connPg);   
                    commandEmp001.CommandTimeout = Convert.ToInt16(ConfigurationManager.AppSettings["CommandTimeOut"]);                                          
                    Stopwatch var_stopwatch = Stopwatch.StartNew();                                                                                             
                    NpgsqlDataReader drPg = commandEmp001.ExecuteReader();                                                                                       
                    var_stopwatch.Stop();                                                                                                                        
                    stopwatch = var_stopwatch.Elapsed.TotalMilliseconds;                                                                                         
                    StreamWriter sw = new StreamWriter(ConfigurationManager.AppSettings["CaminhoArquivo"] + nomeTabelaDestino + ".csv", false);         
                    string separator = ";";   
                    string strRow = "";      
                    for (int i = 0; i < drPg.FieldCount; i++) {    
                        strRow += drPg.GetName(i) + separator;     
                    }       
                    sw.WriteLine(strRow.ToString());      
                    while (drPg.Read()) {               
                        strRow = "";    
                        for (int i = 0; i < drPg.FieldCount; i++) {      
                            strRow = strRow.ToString() + drPg.GetValue(i);  
                            if (i < drPg.FieldCount - 1) {     
                                strRow = strRow.ToString() + separator;    
                            }   
                        }   
                        sw.WriteLine(strRow.ToString());    
                    }   
                    drPg.Close();   
                    sw.Close();    

///// ALERTA DE EXECUTADO COM SUCESSO /////
                    Page page = HttpContext.Current.CurrentHandler as Page;  
                    string script = string.Format("alert('{0}');", "Executado com sucesso!" + "\\nResultados salvos no arquivo: \\n" + ConfigurationManager.AppSettings["CaminhoArquivo"].ToString() + "\\" + nomeTabelaDestino + ".csv");  
                    page.ClientScript.RegisterClientScriptBlock(page.GetType(), "alert", script, true /* addScriptTags */); 
                }                                                                                                                                                ///
                catch (Exception ex) {                                                                                                                           
                    Response.Write(ex.ToString());                                                                                                               
                }   
                finally {  
                    connPg.Close();  
                }  
            }    
///// BUSCA E GRAVAÇÃO DE DADOS DO SQL SERVER /////
            else {                                                                                                                                               
                try {                                                                                                                                           
///// CONEXÃO PARA BUSCAR OS DADOS DA CONSULTA E GRAVAR EM DATAREADER /////
                    SqlConnection connectionExecutaQuery = new SqlConnection(ConfigurationManager.ConnectionStrings[DropDownList_DataSource.SelectedValue].ConnectionString);  
                    connectionExecutaQuery.Open();  
                    SqlCommand command = connectionExecutaQuery.CreateCommand();                                                 
                    command.CommandTimeout = Convert.ToInt16(ConfigurationManager.AppSettings["CommandTimeOut"]);   
                    SqlTransaction transactionExecutaQuery = connectionExecutaQuery.BeginTransaction(IsolationLevel.ReadUncommitted); 
                    command.Connection = connectionExecutaQuery;   
                    command.Transaction = transactionExecutaQuery;  
                    command.CommandText = TextBox_Comando.Text + " OPTION (MAXDOP 1)";  
                    Stopwatch var_stopwatch = Stopwatch.StartNew();   
                    SqlDataReader reader = command.ExecuteReader();   
                    var_stopwatch.Stop();    
                    stopwatch = var_stopwatch.Elapsed.TotalMilliseconds;

///// OPÇÃO PARA GRAVAR O RESULTADO EM ARQUIVO /////
                    if (DropDownList_DataDestination.SelectedValue == "arquivo")
                    {
///// GRAVA RESULTADOS DA QUERY EM OBJETO STREAMWRITER, PARA GRAVAR NO ARQUIVO CSV /////
                        StringBuilder sb = new StringBuilder();                  //CRIA OBJETO PARA ARMAZENAR A STRING A SER GRAVADA NO ARQUIVO .CSV   ///
                        System.IO.Directory.CreateDirectory(ConfigurationManager.AppSettings["CaminhoArquivo"]);                                       ///  CRIA O DIRETÓRIO, SE NÃO EXISTIR
                        StreamWriter sw = new StreamWriter(ConfigurationManager.AppSettings["CaminhoArquivo"]  +  nomeTabelaDestino + ".csv");         ///  CRIA O ARQUIVO .CSV P/ GRAVAR RESULTADO///                 
                        
///// COLOCAR NOME DAS COLUNAS NO CSV /////
                        var columnNames = Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToList();   
                        sb.Append(string.Join(";", columnNames));  
                        sb.AppendLine();  

///// INSERE OS DADOS DO DATAREADER NO CSV /////
                        while (reader.Read()) {  
                            for (int i = 0; i < reader.FieldCount; i++) { 
                                string value = reader[i].ToString();   
                                if (value.Contains(","))   
                                    value = "\"" + value + "\"";   
                                sb.Append(value.Replace(Environment.NewLine, " ") + ";");   
                            }  
                            sb.Length--; //REMOVE A ÚLTIMA VIRGULA  
                            sb.AppendLine();  
                        }      

///// DESALOCA OBJETOS CRIADOS /////
                        sw.Write(sb.ToString());     /// ESCREVE NO ARQUIVO  ///
                        sw.Close();                  /// FECHA O STREAMWRITER (ESCRITA DO ARQUIVO)    ///
                        reader.Close();              /// FECHA O SQL READER                           ///
                        connectionExecutaQuery.Close();   /// FECHA CONEXÃO COM SQL SERVER            ///

///// ALERTA DE EXECUTADO COM SUCESSO /////
                        Page page = HttpContext.Current.CurrentHandler as Page;  
                        string script = string.Format("alert('{0}');", "Executado com sucesso!" + "\\nResultados salvos no arquivo: \\n" + ConfigurationManager.AppSettings["CaminhoArquivo"].ToString() + "\\" + nomeTabelaDestino + ".csv");  
                        page.ClientScript.RegisterClientScriptBlock(page.GetType(), "alert", script, true /* addScriptTags */);  
                    }

///// OPÇÃO PARA GRAVAR O RESULTADO NO SQL SERVER /////
                    if (DropDownList_DataDestination.SelectedValue == "database")
                    {
                        ///////////// CRIA E CARREGA DATATABLE COM DADOS DA QUERY ///////////////
                        DataTable dt = new DataTable();  
                        dt.Load(reader); 

///// CONEXÃO PARA CRIAR A TABELA NO SQL SERVER /////
                        SqlConnection connectionCriaTabelaResultados = new SqlConnection(ConfigurationManager.ConnectionStrings["DestinationDataDEV"].ConnectionString);  
                        connectionCriaTabelaResultados.Open();   
                        SqlCommand commandCriaTabelaResultados = connectionCriaTabelaResultados.CreateCommand();  
                        commandCriaTabelaResultados.CommandText = CreateTABLE(nomeTabelaDestino, dt);   
                        var_stopwatch = Stopwatch.StartNew();  
                        SqlDataReader readerCriaTabelaResultados = commandCriaTabelaResultados.ExecuteReader();    
                        var_stopwatch.Stop();  
                        stopwatch = var_stopwatch.Elapsed.TotalMilliseconds;  
                        connectionCriaTabelaResultados.Close();     
                        

///// CONEXÃO PARA GRAVAR DADOS DA CONSULTA EM TABELA DO SQL SERVER /////
                        SqlConnection destinationConnection = new SqlConnection(ConfigurationManager.ConnectionStrings["DestinationDataDEV"].ConnectionString);    
                        destinationConnection.Open();  
                        SqlBulkCopy bulkCopy = new SqlBulkCopy(destinationConnection);     
                        bulkCopy.DestinationTableName =  nomeTabelaDestino;    
                        bulkCopy.WriteToServer(dt);         
                        
///// DESALOCAR OBJETOS /////
                        bulkCopy.Close();  
                        reader.Close();  
                        destinationConnection.Close();    
                        connectionExecutaQuery.Close();  
                        readerCriaTabelaResultados.Close(); 

///// ALERTA DE EXECUTADO COM SUCESSO /////
                        Page page = HttpContext.Current.CurrentHandler as Page;  
                        string script = string.Format("alert('{0}');", "Executado com sucesso!" + "\\nResultados salvos na tabela: \\n" + nomeTabelaDestino);
                        page.ClientScript.RegisterClientScriptBlock(page.GetType(), "alert", script, true /* addScriptTags */);   
                    }
                }
                catch (Exception ex) {
                    Response.Write(ex.ToString());
                }
            }

///// GRAVA LOG DE EXECUÇÃO NO SQL SERVER /////
            Logs l = new Logs();  // INSTANCIA CLASSE LOGS
            string comandoGravaLog = "EXEC dbo.ExecQueryRegistraLog '" + userName + "', '"                                              /// GET USUARIO CLIENT
               + DropDownList_DataSource.SelectedValue.ToString() + "', '" + TextBox_Comando.Text.ToString().Replace("'", "´")          /// GET DATASOURCE E QUERY
               + "', '" + Request.ServerVariables["REMOTE_HOST"] + "', '"                                                               /// GET CLIENT 
               + Request.Browser.Browser.ToString() + " " + Request.Browser.Version + " - " + Request.Browser.Platform.ToString() + "'" /// GET BROWSER
               + ", " + (int)stopwatch;                                                                                                 /// GET TEMPO EXECUÇÃO
                                                                                                                                        ///
            l.GravaLogSQL(userName, stopwatch, comandoGravaLog);                                                                        
        }


