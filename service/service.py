import requests
import psycopg2
import time

# Configurações do banco de dados PostgreSQL
DB_CONFIG = {
    "dbname": "fipe_db",
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
            id SERIAL PRIMARY KEY,
            codigo INT UNIQUE,
            mes VARCHAR(50)
        );
        
        CREATE TABLE IF NOT EXISTS marcas (
            id SERIAL PRIMARY KEY,
            codigo INT UNIQUE,
            nome VARCHAR(100)
        );

        CREATE TABLE IF NOT EXISTS modelos (
            id SERIAL PRIMARY KEY,
            codigo INT UNIQUE,
            nome VARCHAR(100),
            marca_id INT REFERENCES marcas(codigo)
        );

        CREATE TABLE IF NOT EXISTS anos_modelo (
            id SERIAL PRIMARY KEY,
            codigo VARCHAR(50) UNIQUE,
            descricao VARCHAR(50),
            modelo_id INT REFERENCES modelos(codigo)
        );

        CREATE TABLE IF NOT EXISTS veiculos (
            id SERIAL PRIMARY KEY,
            codigo_fipe VARCHAR(20),
            marca VARCHAR(100),
            modelo VARCHAR(100),
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
        
        if CONTADOR == 10:
            print("Esperando 3 segundos para evitar timeout...")
            time.sleep(3)
            CONTADOR = 0
        else:
            time.sleep(1)
            CONTADOR += 1

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
    conn = conectar_db()
    cur = conn.cursor()
    
    if tabela == "tabela_referencia":
        cur.execute("INSERT INTO tabela_referencia (codigo, mes) VALUES (%s, %s) ON CONFLICT (codigo) DO NOTHING", (dados["Codigo"], dados["Mes"]))
    
    if tabela == "marcas":
        cur.execute("INSERT INTO marcas (codigo, nome) VALUES (%s, %s) ON CONFLICT (codigo) DO NOTHING", (dados["Value"], dados["Label"]))

    elif tabela == "modelos":
        cur.execute("INSERT INTO modelos (codigo, nome, marca_id) VALUES (%s, %s, %s) ON CONFLICT (codigo) DO NOTHING",
                    (dados["Value"], dados["Label"], dados["marca_id"]))

    elif tabela == "anos_modelo":
        cur.execute("INSERT INTO anos_modelo (codigo, descricao, modelo_id) VALUES (%s, %s, %s) ON CONFLICT (codigo) DO NOTHING",
                    (dados["Value"], dados["Label"], dados["modelo_id"]))

    elif tabela == "veiculos":
        # Verifica se o veículo já existe com base no codigo_fipe e mes_referencia
        cur.execute("""
            SELECT id FROM veiculos 
            WHERE codigo_fipe = %s AND mes_referencia = %s
        """, (dados["CodigoFipe"], dados["MesReferencia"]))

        veiculo_existe = cur.fetchone()

        if veiculo_existe:
            # Se o veículo já existe, faz um UPDATE com os dados fornecidos
            cur.execute("""
                UPDATE veiculos 
                SET marca = %s, modelo = %s, ano = %s, combustivel = %s, preco = %s 
                WHERE codigo_fipe = %s AND mes_referencia = %s
            """, (
                dados["Marca"], dados["Modelo"],
                dados["AnoModelo"],
                dados["Combustivel"],
                float(dados["Valor"].replace("R$ ", "").replace(".", "").replace(",", ".")),
                dados["CodigoFipe"],
                dados["MesReferencia"]
            ))
        else:
            # Se o veículo não existe, insere o novo registro
            cur.execute("""
                INSERT INTO veiculos (
                    codigo_fipe,
                    marca,
                    modelo,
                    ano,
                    combustivel,
                    preco,
                    mes_referencia
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                dados["CodigoFipe"],
                dados["Marca"], dados["Modelo"],
                dados["AnoModelo"],
                dados["Combustivel"],
                float(dados["Valor"].replace("R$ ", "").replace(".", "").replace(",", ".")),
                dados["MesReferencia"]
            ))

    conn.commit()
    cur.close()
    conn.close()

# Função principal para rodar o scraping e salvar no banco
def rodar_scraping():
    criar_tabelas()
    codigo_tabela = obter_tabela_referencia()
    
    salvar_no_banco("tabela_referencia", codigo_tabela)
    
    codigo_tabela = codigo_tabela['Codigo']
    # Buscar marcas
    marcas = obter_marcas(codigo_tabela)
    for marca in marcas:
        salvar_no_banco("marcas", marca)

        # Buscar modelos
        modelos = obter_modelos(codigo_tabela, marca["Value"])
        for modelo in modelos:
            modelo["marca_id"] = marca["Value"]
            salvar_no_banco("modelos", modelo)

        for modelo in modelos:
            # Buscar anos disponíveis
            anos_modelo = obter_anos_modelo(codigo_tabela, marca["Value"], modelo["Value"])
            for ano in anos_modelo:
                ano["modelo_id"] = modelo["Value"]
                salvar_no_banco("anos_modelo", ano)

                # Buscar valor do veículo
                valor_veiculo = obter_valor_veiculo(codigo_tabela, marca["Value"], modelo["Value"], ano["Value"])
                
                print(
                    "Marca: " + valor_veiculo['Marca'] + ", " +
                    "Modelo: " + valor_veiculo['Modelo'] + ", " +
                    "Ano: " + str(valor_veiculo['AnoModelo']) + ", " +
                    "Preço: " + str(valor_veiculo['Valor'])
                )
                salvar_no_banco("veiculos", valor_veiculo)

if __name__ == "__main__":
    rodar_scraping()
