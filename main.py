import csv
from datetime import datetime

id_sintegra_inicial = 139  # Valor inicial do ID para produtos_sintegra
id_trib_inicial = 285


def gerar_inserts(modelo_insert_produtos, modelo_insert_sintegra, modelo_insert_trib, csv_path, mapeamento_csv_para_banco, valores_padrao, id_sintegra_inicial, id_trib_inicial):
    """
    Gera INSERTs SQL para as tabelas 'produtos' e 'produtos_sintegra' a partir de um modelo e dados em CSV.

    :param modelo_insert_produtos: String do modelo de INSERT SQL para a tabela 'produtos'.
    :param modelo_insert_sintegra: String do modelo de INSERT SQL para a tabela 'produtos_sintegra'.
    :param csv_path: Caminho para o arquivo CSV.
    :param mapeamento_csv_para_banco: Dicionário mapeando colunas do CSV para as colunas no banco.
    :param valores_padrao: Dicionário com valores padrão para as colunas no banco.
    :return: Lista de comandos SQL gerados para as duas tabelas.
    """
    inserts_produtos = []
    inserts_sintegra = []
    inserts_estado = []
    id_sintegra = id_sintegra_inicial
    id_trib = id_trib_inicial

    with open(csv_path, 'r', encoding='utf-8') as arquivo_csv:
        leitor_csv = csv.DictReader(arquivo_csv, delimiter=',')

        for linha in leitor_csv:
            # Copia os valores padrão
            valores = valores_padrao.copy()

            # Atualiza os valores com base nas colunas do CSV
            for coluna_csv, coluna_banco in mapeamento_csv_para_banco.items():
                if coluna_csv in linha and linha[coluna_csv]:
                    if coluna_csv == "Descricao":
                        valores["DESCRITIVO"] = linha[coluna_csv]
                        # Aplique a limitação de 30 caracteres para DESCRITIVO_PDV
                        valores["DESCRITIVO_PDV"] = linha[coluna_csv][:30]  # Limita a 30 caracteres
                    else:
                        valores[coluna_banco] = linha[coluna_csv]

            if "UNIDADE_VENDA" in valores:
                valores["UNIDADE_COMPRA"] = valores["UNIDADE_VENDA"]

            # Verificação para IPV baseado em UNIDADE_VENDA
            if valores.get("UNIDADE_VENDA") == "KG":
                valores["IPV"] = 0
            else:
                valores["IPV"] = 1

            
            # Gerar INSERT para a tabela 'produtos'
            valores_produtos = {}
            for coluna, valor in valores.items():
                if "DATA" in coluna and valor:
                    valores_produtos[coluna] = f"to_date('{valor}', 'dd-mm-yyyy hh24:mi:ss')"
                elif valor is None:
                    valores_produtos[coluna] = "null"
                else:
                    valores_produtos[coluna] = f"{valor}"
            insert_produtos = modelo_insert_produtos.format(**valores_produtos)
            inserts_produtos.append(insert_produtos)

            # Gerar INSERT para a tabela 'produtos_sintegra'
            valores_sintegra = {
                "ID": id_sintegra,
                "ID_PRODUTO": valores["ID"],
                "DESCRITIVO": valores["DESCRITIVO"],
                "UNIDADE_VENDA": valores["UNIDADE_VENDA"],
                "TRIBUTACAO": valores["TRIBUTACAO"],
                "ICMS": valores["ICMS"],
                "REDUCAO": valores["REDUCAO"],
                "SIT_TRIB": valores["SIT_TRIB"],
                "IPI": valores["IPI"],
                "ESTADO": valores["ESTADO"]
            }
            insert_sintegra = modelo_insert_sintegra.format(**valores_sintegra)
            inserts_sintegra.append(insert_sintegra)
            id_sintegra += 1
            # Gerar INSERT para a tabela 'produtos_estado'
            valores_estado = {
                "ID_TRIB": id_trib,
                "ID_PRODUTO": valores["ID"],
                "TRIBUTACAO": valores["TRIBUTACAO"],
                "ICMS": valores["ICMS"],
                "REDUCAO": valores["REDUCAO"],
                "SIT_TRIB": valores["SIT_TRIB"],
                "ESTADO": valores["ESTADO"],
                "IVA": valores["IVA"],
                "TIPO_IVA": valores["TIPO_IVA"],
            }
            insert_estado = modelo_insert_trib.format(**valores_estado)
            inserts_estado.append(insert_estado)
            #id_trib += 1

    return inserts_produtos, inserts_sintegra, inserts_estado


modelo_insert_produtos = """
INSERT INTO produtos (ID, DESCRITIVO, DATAHORA_CADASTRO, DATAHORA_ALTERACAO, DESCRITIVO_PDV, DEPTO, SECAO, GRUPO, SUBGRUPO, FAMILIA, 
NUTRICIONAL, ASSOCIADO, UNIDADE_COMPRA, UNIDADE_VENDA, STATUS, COMPOSTO, VALIDADE, TP_VALIDADE, RECEITA, CLASSIFICACAO_FISCAL, 
IPI_TIPO, IPI, QTDE_EMBALAGEMS, QTDE_EMBALAGEME, PISCOFINS, IPV, PESQUISA, ACEITAPDVM, ACEITAPDVE, DIASESTOQUE, PESOL, PESOB, SIMILAR, SAZONAL, CESTOQUE, DEL_DEPTO, DEL_SECAO, COR, PANTONE, MPRIMA, DESCRITIVODEL, PONTOS_FIDELIDADE, BASE_FIDELIDADE, SOMENTECD, DESPESA, MONOFASICO, EMITE_ETIQUETA_PEDIDOV, ID_MARCA_PRODUTO, UTILIZA_SERIE, EXCECAO_ICMS, ORIGEM, DESCONTO_MAXIMO, ACEITA_TROCA, MARGEM_CONTRIBUICAO, LOTE_VENDA, ACRESCIMO_FRACIONADO, LICENCA, DESTINADO_MAIORES, ALTO_RISCO, MODO_PREPARO, EXCECAO_IPI, UNIDADE_KGL, QTDE_KGL, CEST, CONSERVACAO, ANTECIPACAO_ARROZ, BENEFICIO_ISENCAO, UTILIZA_ALIQ_INTERNA, LASTRO, CAMADA, PESO_MEDIO, PARTICIPA_COTACAO, MIN_DIAS_VALIDADE, FRACIONADO, CST_IPI, COD_ENQ, LARGURA, ALTURA, TICKET_VINCULADO, CATEGORIA_FISCAL, CONTROLADO, DESCRITIVOQRCODE, MIX_BASE, PROFUNDIDADE, COD_SERVICO_LST, ARMAZENAGEM_ELEVADOR, TIPO_CLASSIFICACAO, REMOVIDO_SORTIMENTO, CONTROLAESTOQUE, DESCRICAO_KGL, COMISSAO, VALIDADE_CONGELADO, TP_VALIDADE_CONGELADO, TARA_BALANCA, CODIGO_PRODUTO_ANVISA, MOTIVO_INSENCAO_ANVISA, IMPRIME_DATA_EMBALAGEM, COMPRIMENTO, EAN_POR_PESO, TAG, ID_USUARIO, PESO_MIN_PDV, PESO_MAX_PDV, VALIDA_PESO_PDV, PERCENTUAL_IMPOSTO_RENDA, QTDE_MAX_MULTIPLICACAO) 
values ({ID}, '{DESCRITIVO}', {DATAHORA_CADASTRO}, {DATAHORA_ALTERACAO}, '{DESCRITIVO_PDV}', {DEPTO}, {SECAO}, {GRUPO}, {SUBGRUPO}, {FAMILIA}, null, null, '{UNIDADE_VENDA}', '{UNIDADE_VENDA}', {STATUS}, 0, 0, 0, null, '{CLASSIFICACAO_FISCAL}', '{IPI_TIPO}', {IPI}, 1.000, 10.000, '{PISCOFINS}', {IPV}, 'F', 'T', 'T', 0, 0.000, 0.000, null, null, 'T', null, null, null, null, null, null, 0.0000, null, 'F', 0.0000, 'O', 'F', null, 'F', null, null, 0.00, 'F', 0.00, 0, 0.00, null, 'F', 'F', null, null, null, 0.000, null, null, null, 'F', 'F', null, null, null, 'F', null, 'F', '{CST_IPI}', null, null, null, null, '00', null, null, 'F', null, null, 'F', null, 'T', 'F', null, null, null, null, null, null, null, 'T', null, '{EAN_POR_PESO}', null, 'ARIUS', null, null, null, null, null);
"""

# Mapeamento das colunas do CSV para as colunas no banco
mapeamento_csv_para_banco = {
    "Codigo": "ID",
    "Descricao": "DESCRITIVO",
    "Ncm": "CLASSIFICACAO_FISCAL",
    "Cest": "CEST",
    #"Ean": "EAN",
    "Unidade": "UNIDADE_VENDA",
    "Tributacao": "TRIBUTACAO",
    "Icms": "ICMS",
    "CstIpiS": "CST_IPI",
    "CstIcmsS": "SIT_TRIB",
}

# Valores padrão para colunas não presentes no CSV
valores_padrao = {
    "DATAHORA_CADASTRO": "27-11-2024 12:00:00",
    "DATAHORA_ALTERACAO": "27-11-2024 12:00:00",
    "UNIDADE_COMPRA": "CX",
    "STATUS": 0,
    "IPI_TIPO": "P",
    "IPI": 0.0000,
    "IPV": 1,
    "DEPTO": 1,
    "SECAO": 3,
    "GRUPO": 0,
    "SUBGRUPO": 0,
    "FAMILIA": 'null',
    "STATUS": 0,
    "PISCOFINS": 'F',
    "EAN_POR_PESO": 'F',
    #TAGS PARA INSERT_TRIB
    "REDUCAO": 0.00,
    "SIT_TRIB": "040",
    "ESTADO": 'MG',
    "IVA": 0.00,
    "TIPO_IVA": 'P',
}

# Modelo de INSERT SQL para a tabela 'produtos_sintegra'
modelo_insert_sintegra = """
INSERT INTO produtos_sintegra (ID, PRODUTO, DESCRITIVO, DATA_INICIAL, UNIDADE_MEDIDA, TRIBUTACAO, ICMS, REDUCAO, SIT_TRIB, IPI, ESTADO, IVA, TIPO_IVA, FCP)
VALUES ({ID}, {ID_PRODUTO}, '{DESCRITIVO}', to_date('27-11-2024 14:44:20', 'dd-mm-yyyy hh24:mi:ss'), '{UNIDADE_VENDA}', '{TRIBUTACAO}', {ICMS}, {REDUCAO}, '{SIT_TRIB}', {IPI}, '{ESTADO}', null, 'P', null);
"""

modelo_insert_trib = """
insert into fis_t_grupo_trib_icms_item (ID_GRUPO_TRIB_ICMS_ITEM, ID_GRUPO_TRIB_ICMS, ID_PRODUTO, DATAHORA_CADASTRO, DATAHORA_ALTERACAO, TRIBUTACAO_VENDA, ICMS_VENDA, REDUCAO_VENDA, ST_VENDA, DECR, CREDITO_ICMS, IVA, TIPO_IVA, FCP, UTILIZA_ALIQ_INTERNA, REGIME_ATACADO_BAHIA, REDUCAO_ICMSST, CODIGO_BENEF_FISCAL, REDUCAO_IVA, VALOR_PAUTA_CARNE_CRED_ICMS, VENDA_CARNE, PERC_DIFERIMENTO, COMPOE_CESTA_BASICA_UF, CODIGO_PRODUTO_GNRE, PRECO_MAX_CONS_MEDICAMENTO, CONFERIDO, ALIQUOTA_ICMS_ORIGINAL, REDUCAO_BC_OPER_PROPRIA_ST, IPI, IPI_TIPO, CST_IPI, COD_ENQ, PERC_CRED_ICMS_CONS_INTERNO)
values ({ID_TRIB}, 1, {ID_PRODUTO}, to_date('28-11-2024 00:00:00', 'dd-mm-yyyy hh24:mi:ss'), to_date('28-11-2024 00:00:00', 'dd-mm-yyyy hh24:mi:ss'), '{TRIBUTACAO}', {ICMS}, {REDUCAO}, '{SIT_TRIB}', 'F', 'F', {IVA}, '{TIPO_IVA}', null, 'F', 'F', 'F', null, null, 0.00000000, 'F', null, 'F', null, null, 'T', null, null, null, null, null, null, null);
"""

# Caminho para o arquivo CSV
caminho_csv = "produtos.csv"

# Gerar os INSERTs
inserts_produtos, inserts_sintegra, inserts_estado = gerar_inserts(
    modelo_insert_produtos, modelo_insert_sintegra, modelo_insert_trib,
    caminho_csv, mapeamento_csv_para_banco, valores_padrao,
    id_sintegra_inicial, id_trib_inicial)

# Salvar os INSERTs no arquivo de saída
with open("inserts_gerados_produtos.sql", "w",
          encoding="utf-8") as arquivo_saida_produtos:
    for insert in inserts_produtos:
        arquivo_saida_produtos.write(insert + "\n")

with open("inserts_gerados_sintegra.sql", "w",
          encoding="utf-8") as arquivo_saida_sintegra:
    for insert in inserts_sintegra:
        arquivo_saida_sintegra.write(insert + "\n")

with open("inserts_produtos_estado.sql", "w",
          encoding="utf-8") as arquivo_saida_estado:
    for insert in inserts_estado:
        arquivo_saida_estado.write(insert + "\n")

print("Inserts gerados e salvos nos arquivos 'inserts_gerados_produtos.sql'")
