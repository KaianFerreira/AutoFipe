import requests
import psycopg2
import time
import os
from supabase import create_client

url = "https://lsrywtmpgxgehinlijky.supabase.co"
key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imxzcnl3dG1wZ3hnZWhpbmxpamt5Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTczODc0NDkzMiwiZXhwIjoyMDU0MzIwOTMyfQ.dyO_cQEQrDxrgcXYY_uwHAtitJvxl4Gezd65U2prjcM"
supabase = create_client(url, key)

# Configurações do banco de dados PostgreSQL
DB_CONFIG = {
    "dbname": "fipe_db3",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5432"
}



# Endpoints da API da FIPE
BASE_URL = "https://veiculos.fipe.org.br/api/veiculos"

# Conectar ao banco de dados
def conectar_db():
    return psycopg2.connect(**DB_CONFIG)

# Criar tabelas no PostgreSQL
def criar_tabelas():
    conn = conectar_db()
    cur = conn.cursor()
    
    cur.execute("""
                
        CREATE TABLE IF NOT EXISTS tabela_referencia (
            codigo int PRIMARY KEY,
            mes VARCHAR(50)
        );
        
        CREATE TABLE IF NOT EXISTS marcas (
            codigo VARCHAR(50) PRIMARY KEY,
            nome VARCHAR(100)
        );

        CREATE TABLE IF NOT EXISTS modelos (
            codigo VARCHAR(50) PRIMARY KEY,
            nome VARCHAR(100),
            marca_id VARCHAR REFERENCES marcas(codigo)
        );

        CREATE TABLE IF NOT EXISTS anos_modelo (
            codigo VARCHAR(50) PRIMARY KEY,
            descricao VARCHAR(50),
            modelo_id VARCHAR REFERENCES modelos(codigo)
        );

        CREATE TABLE IF NOT EXISTS veiculos (
            id SERIAL PRIMARY KEY,
            codigo_fipe VARCHAR(20),
            marca VARCHAR(100),
            modelo_id INT REFERENCES modelos(codigo),
            ano INT,
            combustivel VARCHAR(20),
            preco DECIMAL(10,2),
            mes_referencia INT REFERENCES tabela_referencia(codigo)
        );
    """)
    
    conn.commit()
    cur.close()
    conn.close()

# Faz uma requisição POST à API da FIPE
CONTADOR = 0

def requisitar_api(endpoint, payload):
    global CONTADOR
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }

    try:
        response = requests.post(f"{BASE_URL}/{endpoint}", data=payload, headers=headers)
        time.sleep(1)

        return response.json()
    except requests.exceptions.JSONDecodeError:
        print(f"Erro ao decodificar JSON. Resposta recebida: {response.text}")
        raise

# Obtém a tabela de referência mais recente
def obter_tabela_referencia():
    response = requisitar_api("ConsultarTabelaDeReferencia", {})
    return response[0] # Retorna o código da tabela mais recente

# Obtém todas as marcas de veículos
def obter_marcas(codigo_tabela):
    payload = {
        "codigoTabelaReferencia": codigo_tabela,
        "codigoTipoVeiculo": 1  # 1 = Carro
    }
    return requisitar_api("ConsultarMarcas", payload)

# Obtém todos os modelos de uma marca
def obter_modelos(codigo_tabela, codigo_marca):
    payload = {
        "codigoTabelaReferencia": codigo_tabela,
        "codigoTipoVeiculo": 1,
        "codigoMarca": codigo_marca
    }
    return requisitar_api("ConsultarModelos", payload)["Modelos"]

# Obtém os anos disponíveis de um modelo
def obter_anos_modelo(codigo_tabela, codigo_marca, codigo_modelo):
    payload = {
        "codigoTabelaReferencia": codigo_tabela,
        "codigoMarca": codigo_marca,
        "codigoTipoVeiculo": 1,
        "codigoModelo": codigo_modelo
    }
    return requisitar_api("ConsultarAnoModelo", payload)

# Obtém o valor FIPE de um veículo específico
def obter_valor_veiculo(codigo_tabela, codigo_marca, codigo_modelo, ano_modelo):
    ano, combustivel = ano_modelo.split("-")
    payload = {
        "codigoTabelaReferencia": codigo_tabela,
        "codigoMarca": codigo_marca,
        "codigoModelo": codigo_modelo,
        "codigoTipoVeiculo": 1,
        "anoModelo": ano,
        "codigoTipoCombustivel": combustivel,
        "tipoConsulta": "tradicional"
    }
    
    response = requisitar_api("ConsultarValorComTodosParametros", payload)
    
    response['MesReferencia'] = codigo_tabela
    
    return response

# Insere os dados no banco de dados
def salvar_no_banco(tabela, dados):
    if tabela == "tabela_referencia":
        supabase.table('tabela_referencia').upsert({
            'codigo': dados["Codigo"],
            'mes': dados["Mes"]
        }).execute()
    
    elif tabela == "marcas":
        supabase.table('marcas').upsert({
            'codigo': dados["Value"],
            'nome': dados["Label"]
        }).execute()

    elif tabela == "modelos":
        supabase.table('modelos').upsert({
            'codigo': dados["Value"],
            'nome': dados["Label"],
            'marca_id': dados["marca_id"]
        }).execute()

    elif tabela == "anos_modelo":
        supabase.table('anos_modelo').upsert({
            'codigo': dados["Value"],
            'descricao': dados["Label"],
            'modelo_id': dados["modelo_id"]
        }).execute()

    elif tabela == "veiculos":
        preco = float(dados["Valor"].replace("R$ ", "").replace(".", "").replace(",", "."))
        
        supabase.table('veiculos').upsert({
            'codigo_fipe': dados["CodigoFipe"],
            'marca': dados["Marca"],
            'modelo': dados["Modelo"],
            'ano': dados["AnoModelo"],
            'combustivel': dados["Combustivel"],
            'preco': preco,
            'mes_referencia': dados["MesReferencia"]
        }).execute()

def get_marcas():
    response = supabase.table('marcas').select('codigo', 'nome').execute()
    marcas = response.data
    return [{"Value": marca['codigo'], "Label": marca['nome']} for marca in marcas]

def get_modelos_by_marca(marca_id):
    response = supabase.table('modelos').select('codigo', 'nome').eq('marca_id', marca_id).execute()
    modelos = response.data
    return [{"Value": modelo['codigo'], "Label": modelo['nome']} for modelo in modelos]

def get_anos_by_modelo(modelo_id):
    response = supabase.table('anos_modelo').select('codigo', 'descricao').eq('modelo_id', modelo_id).execute()
    anos = response.data
    return [{"Value": ano['codigo'], "Label": ano['descricao']} for ano in anos]

# Função principal para rodar o scraping e salvar no banco
def rodar_scraping():
    print("\n=== Iniciando processo de scraping ===")
    print("Etapa 1/5: Obtendo referência...")
    codigo_tabela = obter_tabela_referencia()
    
    salvar_no_banco("tabela_referencia", codigo_tabela)
    codigo_tabela = codigo_tabela['Codigo']
    
    print("\nEtapa 2/5: Obtendo marcas...")
    marcas = obter_marcas(codigo_tabela)
    for marca in marcas:
        salvar_no_banco("marcas", marca)
        print(f"✓ Marca processada: {marca['Label']}")

    print("\nEtapa 3/5: Obtendo modelos...")
    total_marcas = len(marcas)
    for i, marca in enumerate(marcas, 1):    
        print(f"\nProcessando modelos da marca {marca['Label']} ({i}/{total_marcas})")
        modelos = obter_modelos(codigo_tabela, marca["Value"])
        
        for modelo in modelos:
            modelo["marca_id"] = marca["Value"]
            salvar_no_banco("modelos", modelo)
            print(f"✓ Modelo salvo: {modelo['Label']}", end='\r')

    print("\nEtapa 4/5: Obtendo anos dos modelos...")
    for i, marca in enumerate(marcas, 1):
        print(f"\nProcessando anos para marca {marca['Label']} ({i}/{total_marcas})")
        modelos = obter_modelos(codigo_tabela, marca["Value"])
        total_modelos = len(modelos)
        
        for j, modelo in enumerate(modelos, 1):
            print(f"Modelo {j}/{total_modelos}: {modelo['Label']}")
            anos_modelo = obter_anos_modelo(codigo_tabela, marca["Value"], modelo["Value"])
            for ano in anos_modelo:
                ano["modelo_id"] = modelo["Value"]
                salvar_no_banco("anos_modelo", ano)
                print(f"✓ Ano processado: {ano['Label']}", end='\r')
    
    print("\nEtapa 5/5: Obtendo valores dos veículos...")
    marcas = get_marcas()
    total_marcas = len(marcas)
    
    for i, marca in enumerate(marcas, 1):
        print(f"\nProcessando valores para marca {marca['Label']} ({i}/{total_marcas})")
        modelos = get_modelos_by_marca(marca["Value"])
        total_modelos = len(modelos)
        
        for j, modelo in enumerate(modelos, 1):
            print(f"Modelo {j}/{total_modelos}: {modelo['Label']}")
            anos = get_anos_by_modelo(modelo["Value"])
            total_anos = len(anos)
            
            for k, ano in enumerate(anos, 1):
                print(f"Processando ano {k}/{total_anos}: {ano['Label']}", end='\r')
                valor_veiculo = obter_valor_veiculo(codigo_tabela, marca["Value"], 
                                                  modelo["Value"], ano["Value"])
                salvar_no_banco("veiculos", valor_veiculo)

    print("\n\n=== Processo de scraping finalizado com sucesso! ===")

if __name__ == "__main__":
    rodar_scraping()
