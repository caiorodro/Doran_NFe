using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Xml;
using Doran_Base;

namespace Doran_NFE_Service
{
    public partial class NFE_Start : ServiceBase
    {
        public NFE_Start()
        {
            InitializeComponent();
        }

        private string pastaApp = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Licenca.xml");
        private string pastaXML = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Pastas.xml");
        private string pastaCertificado = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Certificado.xml");
        private string pastaDANFE = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Danfe.xml");

        private string CNPJ_LICENSA { get; set; }
        private string RAZAO_SOCIAL { get; set; }
        private string LOGOTIPO { get; set; }
        private string SITE { get; set; }
        private string AMBIENTE { get; set; }
        private string UF_EMPRESA { get; set; }
        private string SERIE_NF { get; set; }
        private string CHAVE { get; set; }

        private string CAMINHO_FISICO_PDF { get; set; }
        private string CAMINHO_VIRTUAL_PDF { get; set; }
        private string CAMINHO_FISICO_XML { get; set; }
        private string CAMINHO_RESPOSTAS_XML { get; set; }
        private string XML_ASSINADO { get; set; }
        private string SERVIDOR_SMTP { get; set; }
        private string SERVIDOR_PROXY { get; set; }
        private string USUARIO_PROXY { get; set; }
        private string SENHA_PROXY { get; set; }

        private string CAMINHO_ZIP { get; set; }

        private string EMPRESA_CERTIFICADO { get; set; }
        private string NOME_CERTIFICADO { get; set; }
        private string CNPJ_CERTIFICADO { get; set; }
        private string NUMERO_SERIE_CERTIFICADO { get; set; }
        private string EMISSOR_CERTIFICADO { get; set; }
        private string INICIO_VALIDADE { get; set; }
        private string FIM_VALIDADE { get; set; }

        private System.Timers.Timer timerNFe { get; set; }

        private CultureInfo ci;

        private Th2_Nfe.NF _nf;

        private List<string> id_ufs;

        private int errosNFe = 0;

        private List<CERTIFICADOS> listaCertificados = new List<CERTIFICADOS>();

        enum ICMS
        {
            ICMS00,
            ICMS10,
            ICMS20,
            ICMS30,
            ICMS40,
            ICMS41,
            ICMS50,
            ICMS51,
            ICMS60,
            ICMS70,
            ICMS90,
            ICMSSN101,
            ICMSSN202
        };

        protected override void OnStart(string[] args)
        {
            CultureInfo ci = new CultureInfo(1046);

            System.Threading.Thread.CurrentThread.CurrentCulture = ci;

            CarregaConfiguracoes();
        }

        protected override void OnStop()
        {
        }

        private void CarregaConfiguracoes()
        {
            try
            {
                id_ufs = new List<string>();

                id_ufs.Add("35");
                id_ufs.Add("12");
                id_ufs.Add("27");
                id_ufs.Add("16");
                id_ufs.Add("13");
                id_ufs.Add("29");
                id_ufs.Add("23");
                id_ufs.Add("53");
                id_ufs.Add("32");
                id_ufs.Add("52");
                id_ufs.Add("21");
                id_ufs.Add("51");
                id_ufs.Add("50");
                id_ufs.Add("31");
                id_ufs.Add("15");
                id_ufs.Add("25");
                id_ufs.Add("41");
                id_ufs.Add("26");
                id_ufs.Add("22");
                id_ufs.Add("33");
                id_ufs.Add("24");
                id_ufs.Add("43");
                id_ufs.Add("11");
                id_ufs.Add("14");
                id_ufs.Add("42");
                id_ufs.Add("28");
                id_ufs.Add("17");

                ci = new CultureInfo(1046);
                System.Threading.Thread.CurrentThread.CurrentCulture = ci;

                if (File.Exists(pastaApp))
                {
                    using (DataSet ds = new DataSet())
                    {
                        ds.ReadXml(pastaApp);

                        foreach (DataRow dr in ds.Tables[0].Rows)
                        {
                            CNPJ_LICENSA = dr["CNPJ"].ToString().Trim();
                            RAZAO_SOCIAL = dr["RAZAO_SOCIAL"].ToString().Trim();
                            LOGOTIPO = dr["LOGOTIPO"].ToString().Trim();
                            SITE = dr["SITE"].ToString();
                            AMBIENTE = dr["AMBIENTE"].ToString();

                            if (ds.Tables[0].Columns.Contains("UF_EMPRESA"))
                                UF_EMPRESA = dr["UF_EMPRESA"].ToString();

                            if (ds.Tables[0].Columns.Contains("SERIE_NF"))
                                SERIE_NF = dr["SERIE_NF"].ToString();

                            if (ds.Tables[0].Columns.Contains("CHAVE"))
                                CHAVE = dr["CHAVE"].ToString();
                        }
                    }
                }

                if (File.Exists(pastaXML))
                {
                    using (DataSet ds1 = new DataSet())
                    {
                        ds1.ReadXml(pastaXML);

                        foreach (DataRow dr in ds1.Tables[0].Rows)
                        {
                            CAMINHO_FISICO_PDF = dr["CAMINHO_FISICO_PDF"].ToString().Trim();
                            CAMINHO_VIRTUAL_PDF = dr["CAMINHO_VIRTUAL_PDF"].ToString().Trim();
                            CAMINHO_FISICO_XML = dr["CAMINHO_FISICO_XML"].ToString().Trim();
                            CAMINHO_RESPOSTAS_XML = dr["CAMINHO_RESPOSTA_XML"].ToString().Trim();
                            XML_ASSINADO = dr["XML_ASSINADO"].ToString().Trim();
                            SERVIDOR_SMTP = dr["SERVIDOR_SMTP"].ToString().Trim();
                            SERVIDOR_PROXY = dr["SERVIDOR_PROXY"].ToString().Trim();
                            USUARIO_PROXY = dr["USUARIO_PROXY"].ToString().Trim();
                            SENHA_PROXY = dr["SENHA_PROXY"].ToString().Trim();

                            if (ds1.Tables[0].Columns.Contains("CAMINHO_ZIP"))
                                CAMINHO_ZIP = dr["CAMINHO_ZIP"].ToString().Trim();
                        }
                    }
                }

                if (File.Exists(pastaCertificado))
                {
                    using (DataSet ds2 = new DataSet())
                    {
                        ds2.ReadXml(pastaCertificado);

                        foreach (DataRow dr in ds2.Tables[0].Rows)
                        {
                            EMPRESA_CERTIFICADO = dr["EMPRESA_CERTIFICADO"].ToString().Trim();
                            NOME_CERTIFICADO = dr["NOME_CERTIFICADO"].ToString().Trim();
                            CNPJ_CERTIFICADO = dr["CNPJ_CERTIFICADO"].ToString().Trim();
                            NUMERO_SERIE_CERTIFICADO = dr["NUMERO_SERIE_CERTIFICADO"].ToString().Trim();
                            EMISSOR_CERTIFICADO = dr["EMISSOR_CERTIFICADO"].ToString().Trim();
                            INICIO_VALIDADE = dr["INICIO_VALIDADE_CERTIFICADO"].ToString().Trim();
                            FIM_VALIDADE = dr["FIM_VALIDADE_CERTIFICADO"].ToString().Trim();
                        }
                    }
                }

                _nf = new Th2_Nfe.NF();

                carregaCertificados();

                Verifica_Servico_SEFAZ();

                timerNFe = new System.Timers.Timer(5000);
                timerNFe.Elapsed += timerNFe_Elapsed;
                timerNFe.Enabled = true;
                timerNFe.Start();
            }
            catch (Exception ex)
            {
                Registra_Evento_Servidor("Doran_NFe", "Doran NFe",
                     ex.Message + Environment.NewLine + ex.StackTrace, EventLogEntryType.Error);

                base.Stop();
            }
        }

        void timerNFe_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            timerNFe.Enabled = false;

            try
            {
                using (SqlConnection cnn = new SqlConnection(ConfigurationManager.ConnectionStrings["Th2_DADOS.Properties.Settings.DoranConnectionString"].ConnectionString))
                {
                    cnn.Open();

                    string Sql = "SELECT * FROM TB_FILA_NFE (NOLOCK) WHERE PROCESSADA = 0";

                    SqlCommand command = new SqlCommand(Sql, cnn);

                    SqlDataReader reader = command.ExecuteReader();

                    List<decimal> ID_FILAS = new List<decimal>();
                    List<string> XML_SOLICITACOES = new List<string>();
                    List<string> TIPO = new List<string>();
                    List<decimal> NUMERO_NF = new List<decimal>();
                    List<decimal> ID_EMITENTE = new List<decimal>();

                    while (reader.Read())
                    {
                        ID_FILAS.Add(Convert.ToDecimal(reader["ID_FILA"]));
                        XML_SOLICITACOES.Add(reader["XML_SOLICITACAO"].ToString());
                        TIPO.Add(reader["TIPO_SOLICITACAO"].ToString());
                        NUMERO_NF.Add(Convert.ToDecimal(reader["NUMERO_NF"]));
                        ID_EMITENTE.Add(Convert.ToDecimal(reader["ID_EMITENTE"]));
                    }

                    reader.Close();

                    Sql = "UPDATE TB_FILA_NFE SET PROCESSADA = 1 WHERE ID_FILA = @ID_FILA";
                    command.CommandText = Sql;

                    for (int i = 0; i < ID_FILAS.Count; i++)
                    {
                        command.Parameters.Clear();
                        command.Parameters.AddWithValue("@ID_FILA", ID_FILAS[i]);

                        command.ExecuteNonQuery();
                    }

                    command.Cancel();
                    cnn.Close();

                    for (int i = 0; i < XML_SOLICITACOES.Count; i++)
                    {
                        if (TIPO[i].Equals("NFE"))
                            Processa_NFe_na_Base(XML_SOLICITACOES[i], ID_FILAS[i]);
                        else if (TIPO[i].Equals("CANCELA"))
                            Processa_Cancelamento_NFe(ID_FILAS[i], NUMERO_NF[i], ID_EMITENTE[i], XML_SOLICITACOES[i]);
                        else if (TIPO[i].Equals("CORRECAO"))
                            Processa_Correcao_NFe(XML_SOLICITACOES[i], NUMERO_NF[i], ID_EMITENTE[i], ID_FILAS[i]);
                        else if (TIPO[i].Equals("CONSULTA"))
                            Processa_Consulta_NFe(NUMERO_NF[i], ID_EMITENTE[i], ID_FILAS[i]);
                        else if (TIPO[i].Equals("DEST"))
                            Processa_Consulta_NFe_Destinada(ID_EMITENTE[i], ID_FILAS[i]);
                        else if (TIPO[i].Equals("AMANIFESTO"))
                            Processa_Manifesto_NFe(NUMERO_NF[i], ID_EMITENTE[i], ID_FILAS[i], 0);
                        else if (TIPO[i].Equals("DMANIFESTO"))
                            Processa_Manifesto_NFe(NUMERO_NF[i], ID_EMITENTE[i], ID_FILAS[i], 2);

                    }
                }
            }
            catch (Exception ex)
            {
                Registra_Evento_Servidor("Doran_NFe", "Doran NFe",
                    ex.Message + Environment.NewLine + ex.StackTrace, EventLogEntryType.Error);
            }

            timerNFe.Enabled = true;
        }

        private void Verifica_Servico_SEFAZ()
        {
            if (listaCertificados.Count > 0)
            {
                using (Th2_Nfe.NF nf = new Th2_Nfe.NF())
                {
                    nf.CHAVE_TH2_AGENTE = listaCertificados[0].CHAVE_AGENTE_NFE;

                    nf.SIGLA_UF_EMITENTE = UF_EMPRESA;
                    nf.ambiente = Th2_Nfe.Ambiente.PRODUCAO;

                    nf.NOME_CERTIFICADO = listaCertificados[0].NOME_CERTIFICADO;

                    List<string> x = nf.NFe_ConsultaStatus(out errosNFe);

                    if (x[1].ToLower().Contains("erro") || x[1].ToLower().Contains("rejei"))
                    {
                        Registra_Evento_Servidor("Doran_NFe", "Doran NFe",
                             "Serviço de emissão de notas fiscais eletrônicas (Falhou)!" +
                             Environment.NewLine + Environment.NewLine + x[1], EventLogEntryType.Error);

                    }
                    else
                    {
                        Registra_Evento_Servidor("Doran_NFe", "Doran NFe",
                             "Serviço de emissão de notas fiscais eletrônicas em funcionamento!" +
                             Environment.NewLine + Environment.NewLine + x[1], EventLogEntryType.SuccessAudit);

                    }
                }
            }
        }

        private void carregaCertificados()
        {
            try
            {
                string ARQUIVO = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "caminhoCertificado.txt");

                if (File.Exists(pastaCertificado))
                {
                    using (DataSet ds = new DataSet())
                    {
                        ds.ReadXml(pastaCertificado);

                        foreach (DataRow dr in ds.Tables[0].Rows)
                        {
                            listaCertificados.Add(new CERTIFICADOS()
                            {
                                EMPRESA_CERTIFICADO = dr["EMPRESA_CERTIFICADO"].ToString(),
                                NOME_CERTIFICADO = dr["NOME_CERTIFICADO"].ToString(),
                                CNPJ_CERTIFICADO = dr["CNPJ_CERTIFICADO"].ToString(),
                                NUMERO_SERIE_CERTIFICADO = dr["NUMERO_SERIE_CERTIFICADO"].ToString(),
                                EMISSOR_CERTIFICADO = dr["EMISSOR_CERTIFICADO"].ToString(),
                                INICIO_VALIDADE_CERTIFICADO = dr["INICIO_VALIDADE_CERTIFICADO"].ToString(),
                                FIM_VALIDADE_CERTIFICADO = dr["FIM_VALIDADE_CERTIFICADO"].ToString(),
                                CHAVE_AGENTE_NFE = dr["CHAVE_AGENTE_NFE"].ToString(),
                            });
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Registra_Evento_Servidor("Doran_NFe", "Doran NFe",
                     "Erro ao carregar os certificados!" + Environment.NewLine + ex.Message + Environment.NewLine + ex.StackTrace, EventLogEntryType.Error);
            }
        }

        private void Processa_NFe_na_Base(string XML, decimal ID_FILA)
        {
            try
            {
                NFe_Util_2G.Util nfe = new NFe_Util_2G.Util();

                bool erroXML = false;
                string NOME_CERTIFICADO = string.Empty;

                _nf.CHAVE_TH2_AGENTE = buscaChaveEmitente_na_Base(XML, out erroXML, out NOME_CERTIFICADO);

                if (!erroXML)
                {
                    _nf.NOME_CERTIFICADO = NOME_CERTIFICADO;
                    _nf.CNPJ_EMITENTE = ApoioXML.TrataSinais(CNPJ_CERTIFICADO);

                    string conteudo = XML;

                    string CHAVE = "";

                    using (DataSet ds = new DataSet())
                    {
                        ds.ReadXml(XmlReader.Create(new StringReader(XML)));

                        _nf.SIGLA_UF_EMITENTE = ds.Tables["enderEmit"].Rows[0]["UF"].ToString();
                        _nf.NUMERO_NF = ds.Tables["ide"].Rows[0]["nNF"].ToString();
                        _nf.NUMERO_LOTE = ds.Tables["ide"].Rows[0]["nNF"].ToString();

                        CHAVE = ds.Tables["infNFe"].Rows[0]["Id"].ToString();
                        CHAVE = CHAVE.Replace("NFe", string.Empty);

                        Grava_chave_na_base(ds.Tables["ide"].Rows[0]["nNF"].ToString(), CHAVE);
                    }

                    string ARQUIVO = Path.Combine(XML_ASSINADO, string.Concat("nota", _nf.NUMERO_NF.ToString().PadLeft(9, '0'), "#", _nf.CNPJ_EMITENTE, ".xml"));

                    bool okNF = false;

                    while (!okNF)
                    {
                        try
                        {
                            using (TextWriter tw = new StreamWriter(ARQUIVO))
                            {
                                tw.Write(conteudo);
                                tw.Close();
                            }

                            okNF = true;
                        }
                        catch { }
                    }

                    _nf.ARQUIVO_XML.Remove(0, _nf.ARQUIVO_XML.Length);
                    _nf.ARQUIVO_XML.Append(conteudo);

                    _nf.ARQUIVO_XML_Envio = ARQUIVO;
                    _nf.PASTA_ENVIO = XML_ASSINADO;

                    string resultado = "";
                    string resposta = CAMINHO_RESPOSTAS_XML;

                    _nf.NFe_AssinaXML(out errosNFe, out resultado);

                    if (errosNFe == 0)
                    {
                        using (Th2_DADOS.ORM.DoranDataContext ctx = new Th2_DADOS.ORM.DoranDataContext())
                        {
                            decimal? ID_EMITENTE = (from linha in ctx.TB_FILA_NFEs
                                                    where linha.ID_FILA == ID_FILA
                                                    select linha.ID_EMITENTE).First();

                            Grava_XML_Assinado_na_base(_nf.NUMERO_NF.ToString(), _nf.XML_ASSINADO, 0, ID_EMITENTE.Value);
                        }

                        resposta += resposta.EndsWith("\\") ? "RESPOSTA_NOTA_" + _nf.NUMERO_NF.PadLeft(9, '0') + ".TXT" :
                            "\\RESPOSTA_NOTA_" + _nf.NUMERO_NF.PadLeft(9, '0') + ".TXT";

                        _nf.PASTA_RECEBIMENTO = CAMINHO_RESPOSTAS_XML;
                    }

                    int iResultado = 0;

                    if (errosNFe == 0)
                        resultado = _nf.NFe_ValidaXML(out errosNFe, out iResultado, out resultado);

                    if (errosNFe > 0)
                        throw new Exception(resultado);

                    if (errosNFe == 0)
                        resultado = _nf.NFe_EnviaLote40(out errosNFe);

                    int statusLote = 0;

                    if (errosNFe == 0)
                    {
                        while (true)
                        {
                            System.Threading.Thread.Sleep(5000);

                            string msgResultado = "";
                            string msgRetWS = "";

                            try { resultado = _nf.NFe_BuscaLote40(out errosNFe, out statusLote, out msgResultado, out msgRetWS); }
                            catch { }

                            if (statusLote > 0)
                                if (statusLote != 105)
                                    break;
                        }
                    }

                    if (statusLote == 106)
                    {
                        resultado = "Erro. Lote processado. Consulte a NF em alguns segundos";
                        throw new Exception(resultado);
                    }

                    if (statusLote == 106)
                    {
                        resultado = "Erro. Lote não localizado";
                        throw new Exception(resultado);
                    }

                    if (statusLote == 108)
                    {
                        resultado = "Erro. Serviço paralisado momentaneamente (curto prazo)";
                        throw new Exception(resultado);
                    }

                    if (statusLote == 109)
                    {
                        resultado = "Erro. Serviço paralisado sem previsão";
                        throw new Exception(resultado);
                    }

                    if (errosNFe == 0)
                    {
                        string XML_Protocolo = resultado;

                        if (resultado.IndexOf("<protNFe") == -1)
                        {
                            throw new Exception(resultado);
                        }

                        string protocolo = resultado.Substring(resultado.IndexOf("<protNFe"));
                        protocolo = protocolo.Substring(0, protocolo.IndexOf("</protNFe>") + "</protNFe>".Length);

                        string recibo = resultado.Substring(resultado.IndexOf("<retConsReciNFe"));
                        recibo = recibo.Substring(0, recibo.IndexOf("<protNFe"));

                        XML_Protocolo = XML_Protocolo.Replace(recibo, "");
                        XML_Protocolo = XML_Protocolo.Replace("</retConsReciNFe>", "");

                        string XML_Autorizado = _nf.XML_ASSINADO;

                        XML_Autorizado = @"<nfeProc versao=""4.00"" xmlns=""http://www.portalfiscal.inf.br/nfe"">" + XML_Autorizado + "</nfeProc>";
                        XML_Autorizado = XML_Autorizado.Replace(@"<?xml version=""1.0"" encoding=""utf-8""?>", "");
                        XML_Autorizado = XML_Autorizado.Replace(@"<NFe xmlns=""http://www.portalfiscal.inf.br/nfe"">", "<NFe>");
                        XML_Autorizado = XML_Autorizado.Replace("</nfeProc>", XML_Protocolo + "</nfeProc>");
                        XML_Autorizado = XML_Autorizado.Replace("</nfeProc></nfeProc>", "</nfeProc>");

                        string arquivoPDF = string.Concat(AppDomain.CurrentDomain.BaseDirectory,
                            "nota", _nf.NUMERO_NF.PadLeft(9, '0'), ".pdf");

                        using (Th2_DADOS.ORM.DoranDataContext ctx = new Th2_DADOS.ORM.DoranDataContext())
                        {
                            decimal? ID_EMITENTE = (from linha in ctx.TB_FILA_NFEs
                                                    where linha.ID_FILA == ID_FILA
                                                    select linha.ID_EMITENTE).First();

                            Grava_XML_Assinado_na_base(_nf.NUMERO_NF, XML_Autorizado, ID_FILA, ID_EMITENTE.Value);
                        }

                        int x = 0;

                        string logo = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "logo_padrao.jpg");

                        x = nfe.geraPdfDANFE(XML_Autorizado, logo, "S", "S", "N", "N", "T", arquivoPDF, out resultado);

                        if (!resultado.ToLower().Contains("erro") &&
                            !resultado.ToLower().Contains("rejei"))
                        {
                            using (Th2_DADOS.ORM.DoranDataContext ctx = new Th2_DADOS.ORM.DoranDataContext())
                            {
                                decimal? ID_EMITENTE = (from linha in ctx.TB_FILA_NFEs
                                                        where linha.ID_FILA == ID_FILA
                                                        select linha.ID_EMITENTE).First();

                                byte[] bytes = File.ReadAllBytes(arquivoPDF);
                                GravaPDFnaBase(_nf.NUMERO_NF.ToString(), bytes, ID_EMITENTE.Value);
                                File.Delete(arquivoPDF);
                            }
                        }
                    }

                    if (errosNFe > 0)
                        gravaResposta_na_Base(ID_FILA, resultado);

                    if (errosNFe == 0)
                    {
                        string conteudoResposta = string.Concat("Resultado=", _nf.RESULTADO.Trim().Length == 0 ? resultado : _nf.RESULTADO, Environment.NewLine);
                        conteudoResposta += string.Concat("Mensagem=", _nf.MENSAGEM_RESULTADO, Environment.NewLine);
                        conteudoResposta += string.Concat("Recibo=", _nf.RECIBO, Environment.NewLine);
                        conteudoResposta += string.Concat("ChaveNFe=", _nf.CHAVE_ACESSO, Environment.NewLine);
                        conteudoResposta += string.Concat("NumeroLote=", _nf.NUMERO_LOTE, Environment.NewLine);
                        conteudoResposta += string.Concat("Nprotocolo=", _nf.PROTOCOLO_AUTORIZACAO, Environment.NewLine);

                        conteudoResposta += _nf.DATA_AUTORIZACAO.Length > 9 ?
                            string.Concat("DataAutCanc=", _nf.DATA_AUTORIZACAO.Substring(0, 10), Environment.NewLine) :
                            string.Concat("DataAutCanc=", Environment.NewLine);

                        conteudoResposta += _nf.DATA_AUTORIZACAO.Length > 15 ?
                            string.Concat("HoraAutCanc=", _nf.DATA_AUTORIZACAO.Substring(10, 5)) :
                            "HoraAutCanc=";

                        gravaResposta_na_Base(ID_FILA, conteudoResposta);
                    }
                }
            }
            catch (Exception ex)
            {
                string resultado = ex.Message + Environment.NewLine + ex.StackTrace;

                Registra_Evento_Servidor("Doran_NFe", "Doran NFe",
                    ex.Message + Environment.NewLine + ex.StackTrace, EventLogEntryType.Error);

                gravaResposta_na_Base(ID_FILA, resultado);
            }
        }

        private void GravaPDFnaBase(string NF, byte[] bytes, decimal ID_EMITENTE)
        {
            try
            {
                using (SqlConnection cnn = new SqlConnection(ConfigurationManager.ConnectionStrings["Th2_DADOS.Properties.Settings.DoranConnectionString"].ConnectionString))
                {
                    cnn.Open();

                    string Sql = "UPDATE TB_NOTA_SAIDA SET PDF_DANFE = @DANFE WHERE NUMERO_NF = @NUMERO_NF AND CODIGO_EMITENTE_NF = @ID_EMITENTE";

                    SqlCommand command = new SqlCommand(Sql, cnn);
                    command.Parameters.Add("@DANFE", SqlDbType.Binary).Value = bytes;
                    command.Parameters.Add("@NUMERO_NF", SqlDbType.Decimal).Value = Convert.ToDecimal(NF);
                    command.Parameters.Add("@ID_EMITENTE", SqlDbType.Decimal).Value = ID_EMITENTE;

                    int i = command.ExecuteNonQuery();
                    command.Dispose();

                    cnn.Close();
                }
            }
            catch (Exception ex)
            {
                string resposta = CAMINHO_RESPOSTAS_XML;
                grava_Resposta(resposta, ex.Message);
            }
        }

        private string BuscaXMLnaBase(string NF, decimal ID_EMITENTE)
        {
            string retorno = "";

            try
            {
                using (SqlConnection cnn = new SqlConnection(ConfigurationManager.ConnectionStrings["Th2_DADOS.Properties.Settings.DoranConnectionString"].ConnectionString))
                {
                    cnn.Open();

                    string Sql = "SELECT XML_AUTORIZADO FROM TB_NOTA_SAIDA (NOLOCK) WHERE NUMERO_NF = @NUMERO_NF AND CODIGO_EMITENTE_NF = @ID_EMITENTE";

                    SqlCommand command = new SqlCommand(Sql, cnn);
                    command.Parameters.Add("@NUMERO_NF", SqlDbType.Decimal).Value = Convert.ToDecimal(NF);
                    command.Parameters.Add("@ID_EMITENTE", SqlDbType.Decimal).Value = ID_EMITENTE;

                    SqlDataReader reader = command.ExecuteReader();

                    while (reader.Read())
                    {
                        retorno = reader["XML_AUTORIZADO"].ToString();
                    }

                    reader.Close();
                    command.Dispose();
                    cnn.Close();
                }
            }
            catch (Exception ex)
            {
                string resultado = ex.Message + Environment.NewLine + ex.StackTrace;

                Registra_Evento_Servidor("Doran_NFe", "Doran NFe",
                    ex.Message + Environment.NewLine + ex.StackTrace, EventLogEntryType.Error);
            }

            return retorno;
        }

        private void Grava_chave_na_base(string NF, string CHAVE)
        {
            try
            {
                using (SqlConnection cnn = new SqlConnection(ConfigurationManager.ConnectionStrings["Th2_DADOS.Properties.Settings.DoranConnectionString"].ConnectionString))
                {
                    cnn.Open();

                    string Sql = "UPDATE TB_NOTA_SAIDA SET CHAVE_ACESSO_NF = @CHAVE WHERE NUMERO_NF = @NUMERO_NF";

                    SqlCommand command = new SqlCommand(Sql, cnn);
                    command.Parameters.Add("@CHAVE", SqlDbType.VarChar).Value = CHAVE;
                    command.Parameters.Add("@NUMERO_NF", SqlDbType.Decimal).Value = Convert.ToDecimal(NF);

                    int i = command.ExecuteNonQuery();
                    command.Dispose();

                    cnn.Close();
                }
            }
            catch (Exception ex)
            {
                Registra_Evento_Servidor("Doran_NFe", "Doran NFe",
                    ex.Message + Environment.NewLine + ex.StackTrace, EventLogEntryType.Error);
            }
        }

        private void Grava_XML_Assinado_na_base(string NF, string XML_ASSINADO, decimal ID_FILA, decimal ID_EMIENTE)
        {
            try
            {
                using (SqlConnection cnn = new SqlConnection(ConfigurationManager.ConnectionStrings["Th2_DADOS.Properties.Settings.DoranConnectionString"].ConnectionString))
                {
                    cnn.Open();

                    string Sql = "UPDATE TB_NOTA_SAIDA SET XML_AUTORIZADO = @XML_ASSINADO WHERE NUMERO_NF = @NUMERO_NF AND CODIGO_EMITENTE_NF = @ID_EMITENTE";

                    SqlCommand command = new SqlCommand(Sql, cnn);
                    command.Parameters.Add("@XML_ASSINADO", SqlDbType.Text).Value = XML_ASSINADO;
                    command.Parameters.Add("@NUMERO_NF", SqlDbType.Decimal).Value = Convert.ToDecimal(NF);
                    command.Parameters.Add("@ID_EMITENTE", SqlDbType.Decimal).Value = ID_EMIENTE;

                    int i = command.ExecuteNonQuery();

                    Sql = "UPDATE TB_FILA_NFE SET XML_RESPOSTA = @XML WHERE ID_FILA = @ID_FILA";

                    command.CommandText = Sql;

                    command.Parameters.Clear();
                    command.Parameters.Add("@XML", SqlDbType.Text).Value = XML_ASSINADO;
                    command.Parameters.Add("@ID_FILA", SqlDbType.Decimal).Value = Convert.ToDecimal(ID_FILA);

                    command.ExecuteNonQuery();

                    command.Dispose();

                    cnn.Close();
                }
            }
            catch (Exception ex)
            {
                string resposta = CAMINHO_RESPOSTAS_XML;

                Registra_Evento_Servidor("Doran_NFe", "Doran NFe",
                    ex.Message + Environment.NewLine + ex.StackTrace, EventLogEntryType.Error);

                gravaResposta_na_Base(ID_FILA, ex.Message);
            }
        }

        public string buscaChaveEmitente(string CNPJ_EMITENTE, out bool erroXML, out string NOME_CERTIFICADO)
        {
            string retorno = string.Empty;
            erroXML = false;
            NOME_CERTIFICADO = string.Empty;

            DateTime dt1 = new DateTime();
            DateTime dt2 = new DateTime();

            if (listaCertificados.Where(_ => _.CNPJ_CERTIFICADO == CNPJ_EMITENTE).Any())
            {
                retorno = listaCertificados.Where(_ => _.CNPJ_CERTIFICADO == CNPJ_EMITENTE).First().CHAVE_AGENTE_NFE;

                NOME_CERTIFICADO = listaCertificados.Where(_ => _.CNPJ_CERTIFICADO == CNPJ_EMITENTE).First().NOME_CERTIFICADO;
                EMPRESA_CERTIFICADO = listaCertificados.Where(_ => _.CNPJ_CERTIFICADO == CNPJ_EMITENTE).First().EMPRESA_CERTIFICADO;
                CNPJ_CERTIFICADO = listaCertificados.Where(_ => _.CNPJ_CERTIFICADO == CNPJ_EMITENTE).First().CNPJ_CERTIFICADO;
                NUMERO_SERIE_CERTIFICADO = listaCertificados.Where(_ => _.CNPJ_CERTIFICADO == CNPJ_EMITENTE).First().NUMERO_SERIE_CERTIFICADO;
                EMISSOR_CERTIFICADO = listaCertificados.Where(_ => _.CNPJ_CERTIFICADO == CNPJ_EMITENTE).First().EMISSOR_CERTIFICADO;

                INICIO_VALIDADE = DateTime.TryParse(listaCertificados.Where(_ => _.CNPJ_CERTIFICADO == CNPJ_EMITENTE).First().INICIO_VALIDADE_CERTIFICADO, out dt1) ?
                    Th2_Nfe.Apoio.TrataData2(Convert.ToDateTime(listaCertificados.Where(_ => _.CNPJ_CERTIFICADO == CNPJ_EMITENTE).First().INICIO_VALIDADE_CERTIFICADO)) :
                    "";

                FIM_VALIDADE = DateTime.TryParse(listaCertificados.Where(_ => _.CNPJ_CERTIFICADO == CNPJ_EMITENTE).First().FIM_VALIDADE_CERTIFICADO, out dt2) ?
                    Th2_Nfe.Apoio.TrataData2(Convert.ToDateTime(listaCertificados.Where(_ => _.CNPJ_CERTIFICADO == CNPJ_EMITENTE).First().FIM_VALIDADE_CERTIFICADO)) :
                    "";

                NOME_CERTIFICADO = listaCertificados.Where(_ => _.CNPJ_CERTIFICADO == CNPJ_EMITENTE).First().NOME_CERTIFICADO;
            }

            return retorno;
        }

        public string buscaChaveEmitente_na_Base(string XML, out bool erroXML, out string NOME_CERTIFICADO)
        {
            string retorno = string.Empty;
            erroXML = false;
            NOME_CERTIFICADO = string.Empty;

            if (!File.Exists(pastaCertificado))
            {
                erroXML = true;

                if (File.Exists(Path.Combine(CAMINHO_RESPOSTAS_XML, "erro_certificado.txt")))
                    File.Delete(Path.Combine(CAMINHO_RESPOSTAS_XML, "erro_certificado.txt"));

                grava_Resposta(Path.Combine(CAMINHO_RESPOSTAS_XML, "erro_certificado.txt"), "Não há nenhum certificado digital configurado");
            }
            else
            {
                using (DataSet ds = new DataSet())
                {
                    try
                    {
                        ds.ReadXml(XmlReader.Create(new StringReader(XML)));
                    }
                    catch (Exception ex)
                    {
                        erroXML = true;

                        string resposta = CAMINHO_RESPOSTAS_XML;

                        Registra_Evento_Servidor("Doran_NFe", "Doran NFe",
                            ex.Message + Environment.NewLine + ex.StackTrace, EventLogEntryType.Error);
                    }

                    if (!erroXML)
                    {
                        string CNPJ = string.Empty;

                        DateTime dt1 = new DateTime();
                        DateTime dt2 = new DateTime();

                        int i = 0;

                        foreach (DataRow dr in ds.Tables["emit"].Rows)
                        {
                            if (listaCertificados.Where(_ => _.CNPJ_CERTIFICADO == dr["CNPJ"].ToString()).Any())
                            {
                                retorno = listaCertificados.Where(_ => _.CNPJ_CERTIFICADO == dr["CNPJ"].ToString()).First().CHAVE_AGENTE_NFE;

                                NOME_CERTIFICADO = listaCertificados.Where(_ => _.CNPJ_CERTIFICADO == dr["CNPJ"].ToString()).First().NOME_CERTIFICADO;
                                EMPRESA_CERTIFICADO = listaCertificados.Where(_ => _.CNPJ_CERTIFICADO == dr["CNPJ"].ToString()).First().EMPRESA_CERTIFICADO;
                                CNPJ_CERTIFICADO = listaCertificados.Where(_ => _.CNPJ_CERTIFICADO == dr["CNPJ"].ToString()).First().CNPJ_CERTIFICADO;
                                NUMERO_SERIE_CERTIFICADO = listaCertificados.Where(_ => _.CNPJ_CERTIFICADO == dr["CNPJ"].ToString()).First().NUMERO_SERIE_CERTIFICADO;
                                EMISSOR_CERTIFICADO = listaCertificados.Where(_ => _.CNPJ_CERTIFICADO == dr["CNPJ"].ToString()).First().EMISSOR_CERTIFICADO;

                                INICIO_VALIDADE = DateTime.TryParse(listaCertificados.Where(_ => _.CNPJ_CERTIFICADO == dr["CNPJ"].ToString()).First().INICIO_VALIDADE_CERTIFICADO, out dt1) ?
                                    Th2_Nfe.Apoio.TrataData2(Convert.ToDateTime(listaCertificados.Where(_ => _.CNPJ_CERTIFICADO == dr["CNPJ"].ToString()).First().INICIO_VALIDADE_CERTIFICADO)) :
                                    "";

                                FIM_VALIDADE = DateTime.TryParse(listaCertificados.Where(_ => _.CNPJ_CERTIFICADO == dr["CNPJ"].ToString()).First().FIM_VALIDADE_CERTIFICADO, out dt2) ?
                                    Th2_Nfe.Apoio.TrataData2(Convert.ToDateTime(listaCertificados.Where(_ => _.CNPJ_CERTIFICADO == dr["CNPJ"].ToString()).First().FIM_VALIDADE_CERTIFICADO)) :
                                    "";

                                NOME_CERTIFICADO = listaCertificados.Where(_ => _.CNPJ_CERTIFICADO == dr["CNPJ"].ToString()).First().NOME_CERTIFICADO;
                            }

                            i++;
                        }
                    }

                    if (retorno.Length == 0)
                    {
                        // Th2_Erros.GravaErro(new Exception("Emitente não configurado com o certificado digital"));
                    }
                }
            }

            return retorno;
        }

        private void grava_Resposta(string arquivo, string conteudo)
        {
            using (TextWriter tw = new StreamWriter(arquivo))
            {
                tw.Write(conteudo);
                tw.Close();
            }
        }

        private void gravaResposta_na_Base(decimal ID_FILA, string resultado)
        {
            using (SqlConnection cnn = new SqlConnection(ConfigurationManager.ConnectionStrings["Th2_DADOS.Properties.Settings.DoranConnectionString"].ConnectionString))
            {
                cnn.Open();

                string Sql = "UPDATE TB_FILA_NFE SET RESPOSTA_SEFAZ = @XML WHERE ID_FILA = @ID_FILA";

                SqlCommand command = new SqlCommand(Sql, cnn);
                command.Parameters.AddWithValue("@XML", resultado);
                command.Parameters.AddWithValue("@ID_FILA", ID_FILA);

                command.ExecuteNonQuery();
                cnn.Close();
            }
        }

        private void Processa_Cancelamento_NFe(decimal ID_FILA, decimal NUMERO_NF, decimal ID_EMITENTE, string JUSTIFICATIVA)
        {
            try
            {
                Th2_Nfe.Ambiente ambiente = AMBIENTE == "HOMOLOGAÇÃO" ?
                Th2_Nfe.Ambiente.HOMOLOGACAO :
                Th2_Nfe.Ambiente.PRODUCAO;

                using (Th2_Nfe.NF nf = new Th2_Nfe.NF())
                {
                    string CNPJ_EMITENTE = string.Empty;
                    string PROTOCOLO = string.Empty;
                    string CHAVE = string.Empty;
                    decimal CRT = 0;
                    string NOME_EMITENTE = "";

                    DateTime emissao = DateTime.Now;

                    string _DHEVENTO = string.Concat(emissao.Year.ToString(), "-",
                        emissao.Month.ToString().PadLeft(2, '0'), "-",
                        emissao.Day.ToString().PadLeft(2, '0'), " ",
                        emissao.Hour.ToString().PadLeft(2, '0'), ":",
                        emissao.Minute.ToString().PadLeft(2, '0'), ":",
                        emissao.Second.ToString().PadLeft(2, '0'));

                    using (SqlConnection cnn = new SqlConnection(ConfigurationManager.ConnectionStrings["Th2_DADOS.Properties.Settings.DoranConnectionString"].ConnectionString))
                    {
                        cnn.Open();

                        string Sql = @"SELECT 
        TB_EMITENTE.CNPJ_EMITENTE, 
        TB_EMITENTE.CRT_EMITENTE, 
        TB_NOTA_SAIDA.PROTOCOLO_AUTORIZACAO_NF, 
        TB_NOTA_SAIDA.CHAVE_ACESSO_NF,
        TB_EMITENTE.ID_UF_EMITENTE,
        TB_EMITENTE.NOME_EMITENTE,
        TB_UF.SIGLA_UF
        
        FROM TB_NOTA_SAIDA (NOLOCK) 
        JOIN TB_EMITENTE (NOLOCK) ON TB_NOTA_SAIDA.CODIGO_EMITENTE_NF = TB_EMITENTE.CODIGO_EMITENTE 
        JOIN TB_UF (NOLOCK) ON TB_EMITENTE.ID_UF_EMITENTE = TB_UF.ID_UF
        
        WHERE TB_NOTA_SAIDA.NUMERO_NF = @NOTA AND TB_EMITENTE.CODIGO_EMITENTE = @ID_EMITENTE";

                        SqlCommand command = cnn.CreateCommand();
                        command.CommandText = Sql;
                        command.Parameters.AddWithValue("@NOTA", NUMERO_NF);
                        command.Parameters.AddWithValue("@ID_EMITENTE", ID_EMITENTE);

                        SqlDataReader reader = command.ExecuteReader();

                        while (reader.Read())
                        {
                            CNPJ_EMITENTE = ApoioXML.TrataSinais(reader["CNPJ_EMITENTE"].ToString());
                            PROTOCOLO = reader["PROTOCOLO_AUTORIZACAO_NF"].ToString();
                            CHAVE = reader["CHAVE_ACESSO_NF"].ToString();
                            CRT = Convert.ToDecimal(reader["CRT_EMITENTE"]);
                            NOME_EMITENTE = reader["NOME_EMITENTE"].ToString();
                        }

                        reader.Close();
                        command.Cancel();
                        cnn.Close();
                    }

                    if (string.IsNullOrEmpty(CHAVE))
                    {
                        using (SqlConnection cnn = new SqlConnection(ConfigurationManager.ConnectionStrings["Th2_DADOS.Properties.Settings.DoranConnectionString"].ConnectionString))
                        {
                            string xml = string.Empty;

                            cnn.Open();

                            string cSql = "SELECT XML_AUTORIZADO FROM TB_NOTA_SAIDA (NOLOCK) WHERE NUMERO_NF = @NOTA AND CODIGO_EMITENTE_NF = @ID_EMITENTE";

                            SqlCommand command2 = cnn.CreateCommand();
                            command2.CommandText = cSql;
                            command2.Parameters.AddWithValue("@NOTA", NUMERO_NF);
                            command2.Parameters.AddWithValue("@ID_EMITENTE", ID_EMITENTE);

                            SqlDataReader reader = command2.ExecuteReader();

                            while (reader.Read())
                            {
                                xml = reader["XML_AUTORIZADO"].ToString();
                            }

                            reader.Close();
                            command2.Cancel();
                            cnn.Close();

                            DataSet ds = new DataSet();
                            ds.ReadXml(XmlReader.Create(new StringReader(xml)));

                            CHAVE = ds.Tables["infProt"].Rows[0]["chNFe"].ToString();
                            PROTOCOLO = ds.Tables["infProt"].Rows[0]["nProt"].ToString();
                        }
                    }

                    bool erroXML = false;
                    string NOME_CERTIFICADO = string.Empty;

                    nf.CHAVE_TH2_AGENTE = buscaChaveEmitente(CNPJ_EMITENTE, out erroXML, out NOME_CERTIFICADO);

                    nf.NOME_CERTIFICADO = NOME_CERTIFICADO;
                    nf.NOME_EMITENTE = NOME_EMITENTE;
                    nf.NUMERO_NF = NUMERO_NF.ToString();
                    nf.CRT = (Th2_Nfe.CRT_Emitente)CRT;

                    using (Th2_DADOS.ORM.DoranDataContext ctx = new Th2_DADOS.ORM.DoranDataContext())
                    {
                        string SIGLA_UF_EMITENTE = (from linha in ctx.TB_EMITENTEs
                                                    where linha.CODIGO_EMITENTE == ID_EMITENTE
                                                    select linha.TB_UF.SIGLA_UF).First();

                        string cUF = siglaWS(SIGLA_UF_EMITENTE);

                        string canc = nf.Cancela_NFe(cUF, ambiente, CHAVE.Trim(), PROTOCOLO, JUSTIFICATIVA, _DHEVENTO, out errosNFe);

                        gravaResposta_na_Base(ID_FILA, canc);
                    }
                }
            }
            catch (Exception ex)
            {
                string resposta = CAMINHO_RESPOSTAS_XML;

                Registra_Evento_Servidor("Doran_NFe", "Doran NFe",
                    ex.Message + Environment.NewLine + ex.StackTrace, EventLogEntryType.Error);

                gravaResposta_na_Base(ID_FILA, ex.Message);
            }
        }

        private void Processa_Correcao_NFe(string CORRECAO, decimal NF, decimal ID_EMITENTE, decimal ID_FILA)
        {
            string resposta = CAMINHO_RESPOSTAS_XML;

            try
            {
                Th2_Nfe.Ambiente ambiente = AMBIENTE == "HOMOLOGAÇÃO" ?
                    Th2_Nfe.Ambiente.HOMOLOGACAO :
                    Th2_Nfe.Ambiente.PRODUCAO;

                using (Th2_Nfe.NF nf = new Th2_Nfe.NF())
                {
                    string CNPJ_EMITENTE = string.Empty;
                    string PROTOCOLO = string.Empty;
                    string CHAVE = string.Empty;
                    decimal CRT = 0;
                    decimal NUMERO_CORRECAO = 0;
                    string NOME_EMITENTE = "";
                    string XML_AUTORIZADO = string.Empty;

                    DateTime EMISSAO_NF = new DateTime();

                    using (SqlConnection cnn = new SqlConnection(ConfigurationManager.ConnectionStrings["Th2_DADOS.Properties.Settings.DoranConnectionString"].ConnectionString))
                    {
                        cnn.Open();

                        string Sql = @"SELECT 
        TB_EMITENTE.CNPJ_EMITENTE, 
        TB_EMITENTE.CRT_EMITENTE, 
        TB_NOTA_SAIDA.PROTOCOLO_AUTORIZACAO_NF, 
        TB_NOTA_SAIDA.CHAVE_ACESSO_NF,
        TB_NOTA_SAIDA.DATA_EMISSAO_NF,
        TB_EMITENTE.ID_UF_EMITENTE,
        TB_EMITENTE.NOME_EMITENTE,
        TB_UF.SIGLA_UF,
        TB_NOTA_SAIDA.NUMERO_CORRECAO,
        TB_NOTA_SAIDA.XML_AUTORIZADO
        
        FROM TB_NOTA_SAIDA (NOLOCK) 
        JOIN TB_EMITENTE (NOLOCK) ON TB_NOTA_SAIDA.CODIGO_EMITENTE_NF = TB_EMITENTE.CODIGO_EMITENTE 
        JOIN TB_UF (NOLOCK) ON TB_EMITENTE.ID_UF_EMITENTE = TB_UF.ID_UF
        
        WHERE TB_NOTA_SAIDA.NUMERO_NF = @NOTA AND TB_EMITENTE.CODIGO_EMITENTE = @ID_EMITENTE";

                        SqlCommand command = cnn.CreateCommand();
                        command.CommandText = Sql;
                        command.Parameters.AddWithValue("@NOTA", NF);
                        command.Parameters.AddWithValue("@ID_EMITENTE", ID_EMITENTE);

                        SqlDataReader reader = command.ExecuteReader();

                        while (reader.Read())
                        {
                            CNPJ_EMITENTE = ApoioXML.TrataSinais(reader["CNPJ_EMITENTE"].ToString());
                            PROTOCOLO = reader["PROTOCOLO_AUTORIZACAO_NF"].ToString();
                            CHAVE = reader["CHAVE_ACESSO_NF"].ToString();
                            CRT = Convert.ToDecimal(reader["CRT_EMITENTE"]);
                            NOME_EMITENTE = reader["NOME_EMITENTE"].ToString();
                            XML_AUTORIZADO = reader["XML_AUTORIZADO"].ToString();

                            EMISSAO_NF = Convert.ToDateTime(reader["DATA_EMISSAO_NF"]);

                            NUMERO_CORRECAO = string.IsNullOrEmpty(reader["NUMERO_CORRECAO"].ToString()) ? 0 : Convert.ToDecimal(reader["NUMERO_CORRECAO"]);
                            NUMERO_CORRECAO++;
                        }

                        reader.Close();

                        command.CommandText = "UPDATE TB_NOTA_SAIDA SET NUMERO_CORRECAO = @CORRECAO WHERE NUMERO_NF = @NOTA AND CODIGO_EMITENTE_NF = @ID_EMITENTE";

                        command.Parameters.Clear();
                        command.Parameters.AddWithValue("@CORRECAO", NUMERO_CORRECAO);
                        command.Parameters.AddWithValue("@NOTA", NF);
                        command.Parameters.AddWithValue("@ID_EMITENTE", ID_EMITENTE);
                        command.ExecuteNonQuery();

                        command.Cancel();
                        cnn.Close();

                        if (string.IsNullOrEmpty(CHAVE.Trim()))
                        {
                            DataSet ds = new DataSet();
                            ds.ReadXml(XmlReader.Create(new StringReader(XML_AUTORIZADO)));

                            CHAVE = ds.Tables["infProt"].Rows[0]["chNFe"].ToString();
                            PROTOCOLO = ds.Tables["infProt"].Rows[0]["nProt"].ToString();
                        }
                    }

                    bool erroXML = false;
                    string NOME_CERTIFICADO = string.Empty;

                    nf.CHAVE_TH2_AGENTE = buscaChaveEmitente(CNPJ_EMITENTE, out erroXML, out NOME_CERTIFICADO);
                    nf.NOME_EMITENTE = NOME_EMITENTE;

                    nf.NUMERO_NF = NF.ToString();
                    nf.CRT = (Th2_Nfe.CRT_Emitente)CRT;

                    nf.NOME_CERTIFICADO = NOME_CERTIFICADO;

                    string retWS = "";

                    DateTime emissao = DateTime.Now;

                    string _DHEVENTO = string.Concat(emissao.Year.ToString(), "-",
                        emissao.Month.ToString().PadLeft(2, '0'), "-",
                        emissao.Day.ToString().PadLeft(2, '0'), " ",
                        emissao.Hour.ToString().PadLeft(2, '0'), ":",
                        emissao.Minute.ToString().PadLeft(2, '0'), ":",
                        emissao.Second.ToString().PadLeft(2, '0'));

                    CORRECAO = ApoioXML.TrataSinais(CORRECAO);

                    CORRECAO = CORRECAO.Replace("<", "");
                    CORRECAO = CORRECAO.Replace(">", "");

                    using (Th2_DADOS.ORM.DoranDataContext ctx = new Th2_DADOS.ORM.DoranDataContext())
                    {
                        string SIGLA_UF_EMITENTE = (from linha in ctx.TB_EMITENTEs
                                                    where linha.CODIGO_EMITENTE == ID_EMITENTE
                                                    select linha.TB_UF.SIGLA_UF).First();

                        string cUF = siglaWS(SIGLA_UF_EMITENTE);

                        string correcao = nf.NFe_Carta_de_Correcao(cUF, ambiente, CHAVE.Trim(), PROTOCOLO, CORRECAO, _DHEVENTO, NUMERO_CORRECAO.ToString(), out errosNFe, out retWS);

                        gravaResposta_na_Base(ID_FILA, retWS);
                    }
                }
            }
            catch (Exception ex)
            {
                Registra_Evento_Servidor("Doran_NFe", "Doran NFe",
                    ex.Message + Environment.NewLine + ex.StackTrace, EventLogEntryType.Error);

                gravaResposta_na_Base(ID_FILA, ex.Message + Environment.NewLine + ex.StackTrace);
            }
        }

        private void Processa_Consulta_NFe(decimal NUMERO_NF, decimal ID_EMITENTE, decimal ID_FILA)
        {
            string resposta = CAMINHO_RESPOSTAS_XML;

            try
            {
                Th2_Nfe.Ambiente ambiente = AMBIENTE == "HOMOLOGAÇÃO" ?
                    Th2_Nfe.Ambiente.HOMOLOGACAO :
                    Th2_Nfe.Ambiente.PRODUCAO;

                using (Th2_Nfe.NF nf = new Th2_Nfe.NF())
                {
                    string CNPJ_EMITENTE = string.Empty;
                    string PROTOCOLO = string.Empty;
                    string CHAVE = string.Empty;
                    decimal CRT = 0;
                    string UF_EMITENTE = string.Empty;
                    decimal ID_UF_EMITENTE = 0;
                    string NOME_EMITENTE = string.Empty;
                    string XML = string.Empty;

                    string _DHEVENTO = string.Concat(DateTime.Now.Year.ToString(), "-",
                        DateTime.Now.Month.ToString().PadLeft(2, '0'), "-",
                        DateTime.Now.Day.ToString().PadLeft(2, '0'), " ",
                        DateTime.Now.Hour.ToString().PadLeft(2, '0'), ":",
                        DateTime.Now.Minute.ToString().PadLeft(2, '0'), ":",
                        DateTime.Now.Second.ToString().PadLeft(2, '0'));

                    using (SqlConnection cnn = new SqlConnection(ConfigurationManager.ConnectionStrings["Th2_DADOS.Properties.Settings.DoranConnectionString"].ConnectionString))
                    {
                        cnn.Open();

                        string Sql = @"SELECT 
        TB_EMITENTE.CNPJ_EMITENTE, 
        TB_EMITENTE.CRT_EMITENTE, 
        TB_EMITENTE.NOME_EMITENTE,
        TB_NOTA_SAIDA.PROTOCOLO_AUTORIZACAO_NF, 
        TB_NOTA_SAIDA.CHAVE_ACESSO_NF,
        TB_NOTA_SAIDA.XML_AUTORIZADO,
        TB_EMITENTE.ID_UF_EMITENTE,
        TB_UF.SIGLA_UF
        
        FROM TB_NOTA_SAIDA (NOLOCK) 
        JOIN TB_EMITENTE (NOLOCK) ON TB_NOTA_SAIDA.CODIGO_EMITENTE_NF = TB_EMITENTE.CODIGO_EMITENTE 
        JOIN TB_UF (NOLOCK) ON TB_EMITENTE.ID_UF_EMITENTE = TB_UF.ID_UF
        
        WHERE TB_NOTA_SAIDA.NUMERO_NF = @NOTA AND TB_EMITENTE.CODIGO_EMITENTE = @ID_EMITENTE";

                        SqlCommand command = cnn.CreateCommand();
                        command.CommandText = Sql;
                        command.Parameters.AddWithValue("@NOTA", NUMERO_NF);
                        command.Parameters.AddWithValue("@ID_EMITENTE", ID_EMITENTE);

                        SqlDataReader reader = command.ExecuteReader();

                        while (reader.Read())
                        {
                            CNPJ_EMITENTE = ApoioXML.TrataSinais(reader["CNPJ_EMITENTE"].ToString());
                            PROTOCOLO = reader["PROTOCOLO_AUTORIZACAO_NF"].ToString();
                            CHAVE = reader["CHAVE_ACESSO_NF"].ToString();
                            CRT = Convert.ToDecimal(reader["CRT_EMITENTE"]);
                            ID_UF_EMITENTE = Convert.ToDecimal(reader["ID_UF_EMITENTE"]);
                            UF_EMITENTE = reader["SIGLA_UF"].ToString();
                            NOME_EMITENTE = reader["NOME_EMITENTE"].ToString();
                            XML = reader["XML_AUTORIZADO"].ToString();
                        }

                        reader.Close();
                        command.Cancel();
                        cnn.Close();
                    }

                    if (string.IsNullOrEmpty(CHAVE))
                    {
                        DataSet ds = new DataSet();

                        try
                        {
                            ds.ReadXml(XmlReader.Create(new StringReader(XML)));
                            CHAVE = ds.Tables["infNFe"].Rows[0]["Id"].ToString();
                            ds.Dispose();
                        }
                        catch { }
                    }

                    bool erroXml = false;
                    string NOME_CERTIFICADO = "";

                    nf.NOME_EMITENTE = NOME_EMITENTE;

                    nf.CHAVE_TH2_AGENTE = buscaChaveEmitente(CNPJ_EMITENTE, out erroXml, out NOME_CERTIFICADO);

                    nf.SIGLA_UF_EMITENTE = UF_EMITENTE;
                    nf.ID_UF_IBGE_EMITENTE = ID_UF_EMITENTE.ToString();

                    nf.ambiente = AMBIENTE == "HOMOLOGAÇÃO" ?
                        Th2_Nfe.Ambiente.HOMOLOGACAO :
                        Th2_Nfe.Ambiente.PRODUCAO;

                    nf.NOME_CERTIFICADO = NOME_CERTIFICADO;

                    string xml = string.Empty;
                    string chave = "";
                    using (SqlConnection cnn = new SqlConnection(ConfigurationManager.ConnectionStrings["Th2_DADOS.Properties.Settings.DoranConnectionString"].ConnectionString))
                    {
                        cnn.Open();

                        string cSql = "SELECT XML_AUTORIZADO, CHAVE_ACESSO_NF, STATUS_NF FROM TB_NOTA_SAIDA (NOLOCK) WHERE NUMERO_NF = @NOTA AND CODIGO_EMITENTE_NF = @ID_EMITENTE";

                        SqlCommand command2 = cnn.CreateCommand();
                        command2.CommandText = cSql;
                        command2.Parameters.AddWithValue("@NOTA", NUMERO_NF);
                        command2.Parameters.AddWithValue("@ID_EMITENTE", ID_EMITENTE);

                        SqlDataReader reader = command2.ExecuteReader();

                        while (reader.Read())
                        {
                            xml = reader["XML_AUTORIZADO"].ToString();
                            chave = reader["CHAVE_ACESSO_NF"].ToString();
                        }

                        reader.Close();
                        command2.Cancel();
                        cnn.Close();
                    }

                    nf.CHAVE_ACESSO = chave.Trim();
                    CHAVE = chave.Trim();

                    string retWS = string.Empty;

                    string resultado = nf.NFe_ConsultaNF40(out retWS);

                    bool okStream = false;

                    Stream stream = null;

                    try
                    {
                        stream = Th2_Nfe.Apoio.GenerateStreamFromString(retWS);
                        okStream = true;
                    }
                    catch { }

                    if (retWS.Length > 0)
                    {
                        string Protocolo = string.Empty;
                        string DataAutorizacao = string.Empty;
                        string chaveAcesso = string.Empty;

                        if (okStream)
                        {
                            using (DataSet ds = new DataSet())
                            {
                                ds.ReadXml(stream);

                                if (ds.Tables.Contains("infProt"))
                                {
                                    Protocolo = ds.Tables["infProt"].Columns.Contains("nProt") ?
                                        ds.Tables["infProt"].Rows[0]["nProt"].ToString() : "";

                                    DataAutorizacao = ds.Tables["infProt"].Rows[0]["dhRecbto"].ToString();

                                    chaveAcesso = ds.Tables["infProt"].Rows[0]["chNFe"].ToString();
                                }
                            }
                        }

                        string XML_Protocolo = retWS;

                        if (XML_Protocolo.IndexOf("<protNFe") > -1)
                        {
                            string CNPJ = "";

                            using (SqlConnection cnn = new SqlConnection(ConfigurationManager.ConnectionStrings["Th2_DADOS.Properties.Settings.DoranConnectionString"].ConnectionString))
                            {
                                cnn.Open();

                                string Sql = @"SELECT TB_NOTA_SAIDA.XML_AUTORIZADO, TB_NOTA_SAIDA.STATUS_NF, TB_EMITENTE.CNPJ_EMITENTE FROM TB_NOTA_SAIDA (NOLOCK) 
                                    JOIN TB_EMITENTE (NOLOCK) ON TB_NOTA_SAIDA.CODIGO_EMITENTE_NF = TB_EMITENTE.CODIGO_EMITENTE
                                    WHERE TB_NOTA_SAIDA.NUMERO_NF = @NUMERO_NF AND CODIGO_EMITENTE_NF = @ID_EMITENTE";

                                SqlCommand command = new SqlCommand(Sql, cnn);

                                command.Parameters.Add("@NUMERO_NF", SqlDbType.Decimal).Value = NUMERO_NF;
                                command.Parameters.Add("@ID_EMITENTE", SqlDbType.Decimal).Value = ID_EMITENTE;

                                SqlDataReader reader = command.ExecuteReader();

                                string XML_Autorizado = string.Empty;
                                decimal? STATUS_NF = 0;

                                while (reader.Read())
                                {
                                    STATUS_NF = Convert.ToDecimal(reader["STATUS_NF"]);
                                    XML_Autorizado = reader["XML_AUTORIZADO"].ToString();
                                    CNPJ = ApoioXML.TrataSinais(reader["CNPJ_EMITENTE"].ToString());
                                }

                                reader.Close();
                                command.Cancel();

                                if (STATUS_NF == 2)
                                    STATUS_NF = 4;

                                if (STATUS_NF == 3)
                                    STATUS_NF = 3;

                                if (!XML_Autorizado.Contains("infProt"))
                                {
                                    XML_Protocolo = retWS.Substring(retWS.IndexOf("<protNFe"));
                                    XML_Protocolo = XML_Protocolo.Substring(0, XML_Protocolo.IndexOf("</protNFe>") + "</protNFe>".Length);

                                    XML_Autorizado = @"<nfeProc xmlns=""http://www.portalfiscal.inf.br/nfe"">" + XML_Autorizado + "</nfeProc>";

                                    XML_Autorizado = XML_Autorizado.Replace(@"<?xml version=""1.0"" encoding=""utf-8""?>", "");

                                    XML_Autorizado = XML_Autorizado.Replace(@"<NFe xmlns=""http://www.portalfiscal.inf.br/nfe"">", "<NFe>");

                                    XML_Autorizado = XML_Autorizado.Replace("</nfeProc>", XML_Protocolo + "</nfeProc>");
                                }

                                NFe_Util_2G.Util nfe = new NFe_Util_2G.Util();

                                string arquivoPDF = string.Concat(AppDomain.CurrentDomain.BaseDirectory,
                                    "nota", NUMERO_NF.ToString().PadLeft(9, '0'), ".pdf");

                                string resultado1 = "";

                                string logo = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "logo_padrao.jpg");

                                if (new List<string>() {
                                                    "09179863000142",
                                                    "14572099000100",
                                                    "17617615000164",
                                                    "09179863000304",
                                                    "14572099000291"
                                                }.Contains(CNPJ))
                                {
                                    if (CNPJ == "14572099000100")
                                    { // Cefiro
                                        logo = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Logotipo_Cefiro.png");
                                    }
                                    else if (CNPJ == "17617615000164")
                                    { // Ouro fix
                                        logo = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Ourofix.jpg");
                                    }
                                    else if (CNPJ == "09179863000304") // Lu Ching Filial
                                    {
                                        logo = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "LuChing_Filial.jpg");
                                    }
                                    else if (CNPJ == "14572099000291") // Cefiro Filial
                                    {
                                        logo = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Logotipo_Cefiro_Filial.png");
                                    }
                                }

                                if (new List<string>() { "30920406000153" }.Contains(ApoioXML.TrataSinais(CNPJ)))
                                { // MGF Industria
                                    logo = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "logoMGF.jpg");
                                }

                                if (new List<string>() { "22842094000189" }.Contains(ApoioXML.TrataSinais(CNPJ)))
                                {
                                    logo = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "logoIFX.jpg");
                                }

                                int x = nfe.geraPdfDANFE(XML_Autorizado, logo, "S", "S", "N", "N", "T", arquivoPDF, out resultado1);

                                byte[] bytes = File.ReadAllBytes(arquivoPDF);

                                Sql = @"EXEC sp_configure 'show advanced options', 1 ;
RECONFIGURE;
EXEC sp_configure 'max text repl size', -1;
RECONFIGURE;

UPDATE TB_NOTA_SAIDA SET 
PROTOCOLO_AUTORIZACAO_NF = @PROTOCOLO,
DATA_PROTOCOLO_NF = @DATA_PROTOCOLO,
XML_AUTORIZADO = @XML_AUTORIZADO,
PDF_DANFE = @PDF_DANFE,
CHAVE_ACESSO_NF = @CHAVE_ACESSO,
STATUS_NF = @STATUS
                            
WHERE NUMERO_NF = @NUMERO_NF AND CODIGO_EMITENTE_NF = @ID_EMITENTE";

                                SqlCommand command1 = new SqlCommand(Sql, cnn);
                                command1.Parameters.Add("@PROTOCOLO", SqlDbType.VarChar).Value = Protocolo;
                                command1.Parameters.Add("@DATA_PROTOCOLO", SqlDbType.DateTime).Value = Convert.ToDateTime(DataAutorizacao);
                                command1.Parameters.Add("@XML_AUTORIZADO", SqlDbType.Text).Value = XML_Autorizado;
                                command1.Parameters.Add("@PDF_DANFE", SqlDbType.VarBinary).Value = bytes;
                                command1.Parameters.Add("@CHAVE_ACESSO", SqlDbType.VarChar).Value = CHAVE;
                                command1.Parameters.Add("@STATUS", SqlDbType.Decimal).Value = STATUS_NF;
                                command1.Parameters.Add("@NUMERO_NF", SqlDbType.Decimal).Value = NUMERO_NF;
                                command1.Parameters.Add("@ID_EMITENTE", SqlDbType.Decimal).Value = ID_EMITENTE;

                                int i = command1.ExecuteNonQuery();
                                command1.Dispose();

                                File.Delete(arquivoPDF);

                                cnn.Close();
                            }
                        }
                    }

                    gravaResposta_na_Base(ID_FILA, resultado);
                }
            }
            catch (Exception ex)
            {
                Registra_Evento_Servidor("Doran_NFe", "Doran NFe",
                    ex.Message + Environment.NewLine + ex.StackTrace, EventLogEntryType.Error);

                gravaResposta_na_Base(ID_FILA, ex.Message + Environment.NewLine + ex.StackTrace);
            }
        }

        private void Processa_Consulta_NFe_Destinada(decimal ID_EMITENTE, decimal ID_FILA)
        {
            string resposta = CAMINHO_RESPOSTAS_XML;

            try
            {
                Th2_Nfe.Ambiente ambiente = AMBIENTE == "HOMOLOGAÇÃO" ?
                    Th2_Nfe.Ambiente.HOMOLOGACAO :
                    Th2_Nfe.Ambiente.PRODUCAO;

                using (Th2_Nfe.NF nf = new Th2_Nfe.NF())
                {
                    using (Th2_DADOS.ORM.DoranDataContext ctx = new Th2_DADOS.ORM.DoranDataContext())
                    {
                        string SIGLA_UF_EMITENTE = (from linha in ctx.TB_EMITENTEs
                                                    where linha.CODIGO_EMITENTE == ID_EMITENTE
                                                    select linha.TB_UF.SIGLA_UF).First();

                        string CNPJ_EMITENTE = (from linha in ctx.TB_EMITENTEs
                                                where linha.CODIGO_EMITENTE == ID_EMITENTE
                                                select linha.CNPJ_EMITENTE).First();

                        CNPJ_EMITENTE = ApoioXML.TrataSinais(CNPJ_EMITENTE);

                        string PROTOCOLO = string.Empty;
                        string CHAVE = string.Empty;
                        decimal CRT = 0;
                        string CNPJ_CLIENTE = string.Empty;
                        string NOME_EMITENTE = "";
                        string XML_AUTORIZADO = string.Empty;
                        string ULTIMO_NSU = string.Empty;
                        decimal NUMERO_NF = 0;

                        bool erroXML = false;
                        string NOME_CERTIFICADO = string.Empty;

                        nf.CHAVE_TH2_AGENTE = buscaChaveEmitente(CNPJ_EMITENTE, out erroXML, out NOME_CERTIFICADO);
                        nf.NOME_EMITENTE = NOME_EMITENTE;

                        nf.NUMERO_NF = NUMERO_NF.ToString();
                        nf.CRT = (Th2_Nfe.CRT_Emitente)CRT;

                        nf.NOME_CERTIFICADO = NOME_CERTIFICADO;

                        string retWS = "";

                        DateTime emissao = DateTime.Now;

                        int indNFe = 0;
                        int indEmi = 0;
                        string ultNSU = ULTIMO_NSU;
                        int cStat = 0;

                        var xConsulta = from linha in ctx.TB_CONSULTA_DESTINO_NFEs
                                        where linha.ID_EMITENTE == ID_EMITENTE
                                        select linha;

                        foreach (var item in xConsulta)
                        {
                            ultNSU = item.ULTIMO_NSU.Trim();
                        }

                        if (!xConsulta.Any())
                        {
                            System.Data.Linq.Table<Th2_DADOS.ORM.TB_CONSULTA_DESTINO_NFE> Entidade = ctx.GetTable<Th2_DADOS.ORM.TB_CONSULTA_DESTINO_NFE>();

                            Th2_DADOS.ORM.TB_CONSULTA_DESTINO_NFE novo = new Th2_DADOS.ORM.TB_CONSULTA_DESTINO_NFE();

                            novo.DATA_HORA = DateTime.Now;
                            novo.ID_EMITENTE = ID_EMITENTE;
                            novo.ULTIMO_NSU = "0";
                            novo.RESPOSTA = string.Empty;

                            Entidade.InsertOnSubmit(novo);

                            Doran_Base.Auditoria.Th2_Auditoria.Audita_Insert(ctx, novo, Entidade.ToString(), 1);

                            ctx.saveChanges();

                            ultNSU = novo.ULTIMO_NSU;
                        }

                        NUMERO_NF = (from linha in ctx.TB_FILA_NFEs
                                     where linha.ID_FILA == ID_FILA
                                     select linha.NUMERO_NF).First().Value;

                        string CHAVE_NFE = (from linha in ctx.TB_NOTA_SAIDAs
                                            where linha.NUMERO_NF == NUMERO_NF
                                            && linha.CODIGO_EMITENTE_NF == ID_EMITENTE
                                            select linha.CHAVE_ACESSO_NF).First();

                        string _DHEVENTO = string.Concat(emissao.Year.ToString(), "-",
                            emissao.Month.ToString().PadLeft(2, '0'), "-",
                            emissao.Day.ToString().PadLeft(2, '0'), " ",
                            emissao.Hour.ToString().PadLeft(2, '0'), ":",
                            emissao.Minute.ToString().PadLeft(2, '0'), ":",
                            emissao.Second.ToString().PadLeft(2, '0'));

                        string consulta = nf.Consulta_NFe_Destinada(SIGLA_UF_EMITENTE, ambiente, CHAVE_NFE, _DHEVENTO,
                            out errosNFe, out retWS, indNFe, indEmi, ultNSU, CNPJ_EMITENTE, out ULTIMO_NSU, out cStat);

                        if (string.IsNullOrEmpty(retWS))
                            throw new Exception(consulta);

                        foreach (var item in xConsulta)
                        {
                            item.DATA_HORA = DateTime.Now;
                            item.ID_EMITENTE = ID_EMITENTE;
                            item.ULTIMO_NSU = ULTIMO_NSU;
                            item.RESPOSTA = retWS;

                            ctx.saveChanges();
                        }

                        if (cStat == 137)
                            retWS = "erro: Nenhum documento foi localizado para o destinat&aacute;rio";

                        gravaResposta_na_Base(ID_FILA, retWS);
                    }
                }
            }
            catch (Exception ex)
            {
                Registra_Evento_Servidor("Doran_NFe", "Doran NFe",
                    ex.Message + Environment.NewLine + ex.StackTrace, EventLogEntryType.Error);

                gravaResposta_na_Base(ID_FILA, ex.Message + Environment.NewLine + ex.StackTrace);
            }
        }

        private void Processa_Manifesto_NFe(decimal NUMERO_NF, decimal ID_EMITENTE, decimal ID_FILA, int tipoEvento)
        {
            try
            {
                Th2_Nfe.Ambiente ambiente = AMBIENTE == "HOMOLOGAÇÃO" ?
                    Th2_Nfe.Ambiente.HOMOLOGACAO :
                    Th2_Nfe.Ambiente.PRODUCAO;

                using (Th2_Nfe.NF nf = new Th2_Nfe.NF())
                {
                    using (Th2_DADOS.ORM.DoranDataContext ctx = new Th2_DADOS.ORM.DoranDataContext())
                    {
                        string SIGLA_UF_EMITENTE = (from linha in ctx.TB_EMITENTEs
                                                    where linha.CODIGO_EMITENTE == ID_EMITENTE
                                                    select linha.TB_UF.SIGLA_UF).First();

                        string CNPJ_EMITENTE = (from linha in ctx.TB_EMITENTEs
                                                where linha.CODIGO_EMITENTE == ID_EMITENTE
                                                select linha.CNPJ_EMITENTE).First();

                        CNPJ_EMITENTE = ApoioXML.TrataSinais(CNPJ_EMITENTE);

                        decimal CRT = 0;
                        string CNPJ_CLIENTE = string.Empty;
                        string NOME_EMITENTE = "";
                        string XML_AUTORIZADO = string.Empty;
                        string ULTIMO_NSU = string.Empty;
                        int cStat = 0;

                        bool erroXML = false;
                        string NOME_CERTIFICADO = string.Empty;

                        nf.CHAVE_TH2_AGENTE = buscaChaveEmitente(CNPJ_EMITENTE, out erroXML, out NOME_CERTIFICADO);
                        nf.NOME_EMITENTE = NOME_EMITENTE;

                        nf.CRT = (Th2_Nfe.CRT_Emitente)CRT;

                        nf.NOME_CERTIFICADO = NOME_CERTIFICADO;

                        string retWS = "";

                        string manifesto = ""; // nf.Manifesta_NFe(SIGLA_UF_EMITENTE, ambiente, out retWS, tipoEvento, CHAVE, out cStat);

                        DataSet ds = new DataSet();

                        if (string.IsNullOrEmpty(retWS))
                            throw new Exception(manifesto);
                        else
                        {
                            bool erro = false;

                            try { ds.ReadXml(XmlReader.Create(new StringReader(retWS))); }
                            catch { erro = true; }

                            if (!erro)
                            {
                                string xm = ds.Tables["infEvento"].Rows[0]["xMotivo"].ToString();

                                if (xm.StartsWith("Rejei"))
                                    throw new Exception(xm);
                            }
                        }

                        gravaResposta_na_Base(ID_FILA, retWS);

                        System.Data.Linq.Table<Th2_DADOS.ORM.TB_MANIFESTO_NFE> Entidade1 = ctx.GetTable<Th2_DADOS.ORM.TB_MANIFESTO_NFE>();

                        Th2_DADOS.ORM.TB_MANIFESTO_NFE novo1 = new Th2_DADOS.ORM.TB_MANIFESTO_NFE();

                        novo1.DATA_HORA = DateTime.Now;
                        novo1.CHAVE_NFE = ds.Tables["infEvento"].Rows[0]["chNFe"].ToString();
                        novo1.PROTOCOLO_NFE = "";
                        novo1.DATA_PROTOCOLO = DateTime.Today;
                        novo1.NUMERO_SEQ_NFE = 0;
                        novo1.TIPO_EVENTO = tipoEvento;
                        novo1.RESPOSTA = retWS;

                        Entidade1.InsertOnSubmit(novo1);

                        Doran_Base.Auditoria.Th2_Auditoria.Audita_Insert(ctx, novo1, Entidade1.ToString(), 1);

                        ctx.saveChanges();
                    }
                }
            }
            catch (Exception ex)
            {
                Registra_Evento_Servidor("Doran_NFe", "Doran NFe",
                    ex.Message + Environment.NewLine + ex.StackTrace, EventLogEntryType.Error);

                gravaResposta_na_Base(ID_FILA, ex.Message + Environment.NewLine + ex.StackTrace);
            }
        }

        public string soNumeros(string str1)
        {
            string numeros = "0123456789";
            string retorno = "";

            for (int i = 0; i < str1.Length; i++)
            {
                if (numeros.Contains(str1.Substring(i, 1)))
                    retorno += str1.Substring(i, 1);
            }

            return retorno;
        }

        private void Registra_Evento_Servidor(string Nome_Evento, string Titulo_Evento, string Mensagem, System.Diagnostics.EventLogEntryType tipo)
        {
            if (!EventLog.SourceExists(Nome_Evento))
                EventLog.CreateEventSource(Nome_Evento, Titulo_Evento);

            EventLog.WriteEntry(Nome_Evento, Mensagem, tipo, 236);
        }

        private string siglaWS(string siglaUFEmitente)
        {
            Dictionary<string, string> retorno = new Dictionary<string, string>();

            retorno.Add("SP", "SP");
            retorno.Add("SC", "SVRS");

            return retorno[siglaUFEmitente];
        }
    }

    public class CERTIFICADOS
    {
        public CERTIFICADOS() { }

        public string EMPRESA_CERTIFICADO { get; set; }
        public string NOME_CERTIFICADO { get; set; }
        public string CNPJ_CERTIFICADO { get; set; }
        public string NUMERO_SERIE_CERTIFICADO { get; set; }
        public string EMISSOR_CERTIFICADO { get; set; }
        public string INICIO_VALIDADE_CERTIFICADO { get; set; }
        public string FIM_VALIDADE_CERTIFICADO { get; set; }
        public string CHAVE_AGENTE_NFE { get; set; }
    }
}
