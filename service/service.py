import httpx
import os
import asyncio
from aiolimiter import AsyncLimiter
from supabase import create_client
import aiohttp  # Adicione esta importação no topo do arquivo
import time
from collections import deque
from datetime import datetime
from functools import wraps

url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")
supabase = create_client(url, key)


# Endpoints da API da FIPE
BASE_URL = "https://veiculos.fipe.org.br/api/veiculos"
HEADERS = {
    "Content-Type": "application/x-www-form-urlencoded"
}

# Adicione estas variáveis globais no início do arquivo, após as importações
api_calls_counter = 0
api_calls_by_endpoint = {}

class RateLimitTester:
    def __init__(self):
        self.requests_history = deque(maxlen=100)
        self.errors_429 = 0
        self.success_count = 0
        self.start_time = None
    
    def add_request(self, success):
        now = datetime.now()
        if not self.start_time:
            self.start_time = now
        
        self.requests_history.append((now, success))
        if success:
            self.success_count += 1
        else:
            self.errors_429 += 1
    
    def get_stats(self):
        if not self.requests_history:
            return "Sem dados suficientes"
        
        duration = (datetime.now() - self.start_time).total_seconds()
        total_requests = len(self.requests_history)
        success_rate = (self.success_count / total_requests) * 100 if total_requests > 0 else 0
        
        if len(self.requests_history) >= 2:
            recent_requests = list(self.requests_history)[-2:]
            time_diff = (recent_requests[1][0] - recent_requests[0][0]).total_seconds()
            current_rate = 1 / time_diff if time_diff > 0 else 0
        else:
            current_rate = 0
        
        return {
            "total_requests": total_requests,
            "success_rate": f"{success_rate:.2f}%",
            "errors_429": self.errors_429,
            "requests_per_second": f"{current_rate:.2f}",
            "duration_seconds": f"{duration:.2f}"
        }

rate_tester = RateLimitTester()
rate_limit = AsyncLimiter(5, 10)  # Ajuste estes valores durante os testes

async def requisitar_api(endpoint, payload):
    global api_calls_counter, api_calls_by_endpoint
    max_retries = 3
    retry_delay = 2
    
    for attempt in range(max_retries):
        async with rate_limit:
            try:
                api_calls_counter += 1
                api_calls_by_endpoint[endpoint] = api_calls_by_endpoint.get(endpoint, 0) + 1
                
                async with aiohttp.ClientSession() as session:
                    async with session.post(f"{BASE_URL}/{endpoint}", data=payload, headers=HEADERS) as response:
                        if response.status == 429:
                            rate_tester.add_request(False)
                            wait_time = (attempt + 1) * retry_delay
                            print(f"\nRate limit atingido! Estatísticas:")
                            print(rate_tester.get_stats())
                            print(f"Aguardando {wait_time} segundos...")
                            await asyncio.sleep(wait_time)
                            continue
                        
                        rate_tester.add_request(True)
                        if api_calls_counter % 10 == 0:
                            print("\nEstatísticas de requisições:")
                            print(rate_tester.get_stats())
                        
                        response.raise_for_status()
                        return await response.json()
                        
            except aiohttp.ClientError as e:
                print(f"Erro na requisição: {e}. Tentativa {attempt + 1} de {max_retries}")
                if attempt == max_retries - 1:
                    raise
                await asyncio.sleep(retry_delay)
            except ValueError as e:
                print(f"Erro ao decodificar JSON. Resposta recebida: {await response.text()}")
                raise
    
    return None

# Obtém a tabela de referência mais recente
async def obter_tabela_referencia():
    response = await requisitar_api("ConsultarTabelaDeReferencia", {})
    return response[0] # Retorna o código da tabela mais recente

# Obtém todas as marcas de veículos
async def obter_marcas(codigo_tabela):
    payload = {
        "codigoTabelaReferencia": codigo_tabela,
        "codigoTipoVeiculo": 1  # 1 = Carro
    }
    return await requisitar_api("ConsultarMarcas", payload)

# Obtém todos os modelos de uma marca
async def obter_modelos(codigo_tabela, codigo_marca):
    payload = {
        "codigoTabelaReferencia": codigo_tabela,
        "codigoTipoVeiculo": 1,
        "codigoMarca": codigo_marca
    }
    response = await requisitar_api("ConsultarModelos", payload)
    return response["Modelos"]

# Obtém os anos disponíveis de um modelo
async def obter_anos_modelo(codigo_tabela, codigo_marca, codigo_modelo):
    payload = {
        "codigoTabelaReferencia": codigo_tabela,
        "codigoMarca": codigo_marca,
        "codigoTipoVeiculo": 1,
        "codigoModelo": codigo_modelo
    }
    resultado = await requisitar_api("ConsultarAnoModelo", payload)
    if resultado is None:
        print(f"Falha ao obter anos para modelo {codigo_modelo}. Pulando...")
        return []
    return resultado

# Obtém o valor FIPE de um veículo específico
async def obter_valor_veiculo(codigo_tabela, codigo_marca, codigo_modelo, ano_modelo):
    print(codigo_tabela, codigo_marca, codigo_modelo, ano_modelo)
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
    
    response = await requisitar_api("ConsultarValorComTodosParametros", payload)
    
    response['MesReferencia'] = codigo_tabela
    
    return response

def retry_on_connection_error(max_retries=3, delay=1):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except (httpx.RemoteProtocolError, httpx.ReadTimeout) as e:
                    if attempt == max_retries - 1:
                        raise
                    print(f"\nErro de conexão com Supabase: {e}")
                    print(f"Tentativa {attempt + 1} de {max_retries}. Aguardando {delay} segundos...")
                    time.sleep(delay * (attempt + 1))
            return None
        return wrapper
    return decorator

@retry_on_connection_error()
def salvar_no_banco(tabela, dados):
    if tabela == "tabela_referencia":
        response = supabase.table('tabela_referencia').upsert({
            'codigo': dados["Codigo"],
            'mes': dados["Mes"]
        }).execute()
    
    elif tabela == "marcas":
        response = supabase.table('marcas').upsert({
            'codigo': dados["Value"],
            'nome': dados["Label"]
        }).execute()

    elif tabela == "modelos":
        response = supabase.table('modelos').upsert({
            'codigo': dados["Value"],
            'nome': dados["Label"],
            'marca_id': dados["marca_id"]
        }).execute()

    elif tabela == "anos_modelo":
        response = supabase.table('anos_modelo').upsert({
            'codigo': dados["Value"],
            'descricao': dados["Label"],
            'modelo_id': dados["modelo_id"]
        }).execute()

    elif tabela == "veiculos":
        response = supabase.table('veiculos').upsert({
            'marca_id': dados["marca_id"],
            'modelo_id': dados["modelo_id"],
            'ano_id': dados["ano_id"],
            'mes_referencia_id': dados["mes_referencia_id"],
            'codigo_fipe': dados["codigo_fipe"],
            'combustivel': dados["combustivel"],
            'preco': dados["preco"],
        }).execute()
    
    return response

def get_mes_referencia():
    response = supabase.table('tabela_referencia').select('*').execute()
    mes_referencia = response.data
    return [{"Value": mes_referencia['codigo'], "Label": mes_referencia['mes']} for mes_referencia in mes_referencia]

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

def clear_console():
    # Limpa a tela do console (funciona em Windows e Unix)
    os.system('cls' if os.name == 'nt' else 'clear')

async def rodar_scraping():
    print("\n=== Iniciando processo de scraping ===")
    
    def update_progress(etapa, detalhe="", progresso=""):
        clear_console()
        print(f"=== Processo de Scraping em Andamento ===")
        print(f"Etapa atual: {etapa}/5")
        if detalhe:
            print(f"Processando: {detalhe}")
        if progresso:
            print(f"Progresso: {progresso}")
        print("\nEstatísticas de requisições:")
        print(rate_tester.get_stats())
        print("\nChamadas por endpoint:")
        print(api_calls_by_endpoint)
    
    update_progress("1", "Obtendo referência")
    codigo_tabela = await obter_tabela_referencia()
    
    salvar_no_banco("tabela_referencia", codigo_tabela)
    codigo_tabela = codigo_tabela['Codigo']
    
    update_progress("2", "Obtendo marcas")
    # marcas = await obter_marcas(codigo_tabela)
    # total_marcas = len(marcas)
    # for i, marca in enumerate(marcas, 1):
    #     update_progress("2", f"Marca: {marca['Label']}", f"{i}/{total_marcas}")
    #     salvar_no_banco("marcas", marca)

    update_progress("3", "Obtendo modelos")
    # for i, marca in enumerate(marcas, 1):
    #     modelos = await obter_modelos(codigo_tabela, marca["Value"])
    #     total_modelos = len(modelos)
        
    #     for j, modelo in enumerate(modelos, 1):
    #         update_progress("3", 
    #                       f"Marca: {marca['Label']} ({i}/{total_marcas})",
    #                       f"Modelo: {modelo['Label']} ({j}/{total_modelos})")
    #         modelo["marca_id"] = marca["Value"]
    #         salvar_no_banco("modelos", modelo)

    update_progress("4", "Obtendo anos dos modelos")
    marcas = get_marcas()
    total_marcas = len(marcas)
    for i, marca in enumerate(marcas, 1):
        modelos = get_modelos_by_marca(marca["Value"])
        total_modelos = len(modelos)
        
        for j, modelo in enumerate(modelos, 1):
            anos_modelo = await obter_anos_modelo(codigo_tabela, marca["Value"], modelo["Value"])
            total_anos = len(anos_modelo)
            
            for k, ano in enumerate(anos_modelo, 1):
                update_progress("4",
                              f"Marca: {marca['Label']} ({i}/{total_marcas})",
                              f"Modelo: {modelo['Label']} ({j}/{total_modelos}) - Ano: {ano['Label']} ({k}/{total_anos})")
                ano["modelo_id"] = modelo["Value"]
                salvar_no_banco("anos_modelo", ano)
    
    update_progress("5", "Obtendo valores dos veículos")
    meses = get_mes_referencia()
    
    for mes_idx, mes_referencia in enumerate(meses, 1):
        marcas = get_marcas()
        total_marcas = len(marcas)
        
        for i, marca in enumerate(marcas, 1):
            modelos = get_modelos_by_marca(marca["Value"])
            total_modelos = len(modelos)
            
            for j, modelo in enumerate(modelos, 1):
                anos = get_anos_by_modelo(modelo["Value"])
                total_anos = len(anos)
                
                for k, ano in enumerate(anos, 1):
                    update_progress("5",
                                  f"Mês: {mes_idx}/{len(meses)} - Marca: {marca['Label']} ({i}/{total_marcas})",
                                  f"Modelo: {modelo['Label']} ({j}/{total_modelos}) - Ano: {ano['Label']} ({k}/{total_anos})")
                    
                    valor_veiculo = await obter_valor_veiculo(
                        codigo_tabela,
                        marca["Value"],
                        modelo["Value"],
                        ano["Value"]
                    )
                    
                    if valor_veiculo:
                        salvar_no_banco("veiculos", {
                            'marca_id': marca["Value"],
                            'modelo_id': modelo["Value"],
                            'ano_id': ano["Value"],
                            'mes_referencia_id': mes_referencia["Value"],
                            'codigo_fipe': valor_veiculo["CodigoFipe"],
                            'combustivel': valor_veiculo["Combustivel"],
                            'preco': float(valor_veiculo["Valor"].replace("R$ ", "").replace(".", "").replace(",", ".")),
                        })

    clear_console()
    print("\n\n=== Processo de scraping finalizado com sucesso! ===")

if __name__ == "__main__":
    asyncio.run(rodar_scraping())
