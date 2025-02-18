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
import traceback

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

# Configuração de amostragem
MODO_AMOSTRAGEM = False  # Altere para False para processar todos os dados

# Limites de amostragem (só serão usados se MODO_AMOSTRAGEM for True)
LIMITES = {
    'marcas': 3,
    'modelos': 2,
    'anos': 2,
    'meses': 1
}

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
async def obter_valor_veiculo(codigo_tabela, codigo_marca, codigo_modelo, ano_data):
    try:
        print(f"\nIniciando consulta de valor para:")
        print(f"- Tabela: {codigo_tabela}")
        print(f"- Marca: {codigo_marca}")
        print(f"- Modelo: {codigo_modelo}")
        print(f"- Ano/Combustível: {ano_data['codigo']}")
        
        ano, combustivel = ano_data['codigo'].split("-")
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
        if not response:
            print(f"❌ Erro: Resposta vazia para veículo {codigo_modelo} ano {ano_data['codigo']}")
            return None
            
        print(f"✅ Resposta da API recebida:")
        print(f"- Código FIPE: {response.get('CodigoFipe', 'N/A')}")
        print(f"- Valor: {response.get('Valor', 'N/A')}")
        print(f"- Combustível: {response.get('Combustivel', 'N/A')}")
        
        try:
            preco = float(response["Valor"].replace("R$ ", "").replace(".", "").replace(",", "."))
            dados_veiculo = {
                'modelo_id': int(codigo_modelo),  # Mantemos apenas o modelo_id
                'ano_id': ano_data['id'],        # e o ano_id
                'mes_referencia_id': int(codigo_tabela),
                'codigo_fipe': response["CodigoFipe"],
                'combustivel': response["Combustivel"],
                'preco': preco,
            }
            
            print("\n📝 Dados preparados para inserção:")
            for campo, valor in dados_veiculo.items():
                print(f"- {campo}: {valor} ({type(valor)})")
            
            resultado = salvar_no_banco("veiculos", dados_veiculo)
            if resultado and resultado.data:
                
                print("✅ Veículo salvo com sucesso!")
            else:
                print("⚠️ Veículo não foi salvo (possível duplicata ou erro)")
            
            return response
            
        except Exception as e:
            print(f"❌ Erro ao preparar dados: {str(e)}")
            print(f"Response original: {response}")
            return None
            
    except Exception as e:
        print(f"❌ Erro em obter_valor_veiculo: {str(e)}")
        return None

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
    try:
        print(f"\n🔄 Iniciando salvamento na tabela {tabela}")
        
        if tabela == "veiculos":
            print("Verificando existência do veículo...")
            try:
                # Verifica se todos os campos necessários estão presentes
                campos_obrigatorios = ['modelo_id', 'ano_id', 'mes_referencia_id', 'codigo_fipe', 'combustivel', 'preco']
                for campo in campos_obrigatorios:
                    if campo not in dados:
                        print(f"❌ Campo obrigatório ausente: {campo}")
                        return None
                
                # Verifica tipos dos dados
                print("\nVerificando tipos dos dados:")
                print(f"modelo_id: {type(dados['modelo_id'])} = {dados['modelo_id']}")
                print(f"ano_id: {type(dados['ano_id'])} = {dados['ano_id']}")
                print(f"mes_referencia_id: {type(dados['mes_referencia_id'])} = {dados['mes_referencia_id']}")
                print(f"codigo_fipe: {type(dados['codigo_fipe'])} = {dados['codigo_fipe']}")
                print(f"combustivel: {type(dados['combustivel'])} = {dados['combustivel']}")
                print(f"preco: {type(dados['preco'])} = {dados['preco']}")
                
                # Verifica existência
                existing = supabase.table('veiculos')\
                    .select('*')\
                    .eq('modelo_id', dados['modelo_id'])\
                    .eq('ano_id', dados['ano_id'])\
                    .eq('mes_referencia_id', dados['mes_referencia_id'])\
                    .execute()
                
                print(f"\nResultado da verificação de existência: {existing.data}")
                
                if existing and len(existing.data) > 0:
                    print("⚠️ Veículo já existe no banco, pulando...")
                    return existing
                
                print("\nTentando inserir novo veículo...")
                try:
                    response = supabase.table('veiculos').insert(dados).execute()
                    print(f"Resposta da inserção: {response}")
                    
                    # Verifica se a inserção foi bem-sucedida
                    if response and response.data:
                        print("✅ Veículo inserido com sucesso!")
                        
                        # Verifica se realmente foi inserido
                        verificacao = supabase.table('veiculos')\
                            .select('*')\
                            .eq('modelo_id', dados['modelo_id'])\
                            .eq('ano_id', dados['ano_id'])\
                            .eq('mes_referencia_id', dados['mes_referencia_id'])\
                            .execute()
                            
                        print(f"Verificação pós-inserção: {verificacao.data}")
                    else:
                        print("❌ Falha na inserção: response vazia ou sem dados")
                    
                    return response
                    
                except Exception as e:
                    print(f"❌ Erro na inserção: {str(e)}")
                    print("Tentando debug da conexão...")
                    try:
                        # Tenta uma query simples para verificar a conexão
                        test = supabase.table('veiculos').select('count').execute()
                        print(f"Teste de conexão: {test}")
                    except Exception as e2:
                        print(f"❌ Erro no teste de conexão: {str(e2)}")
                    raise
                
            except Exception as e:
                print(f"❌ Erro específico na operação com veículos: {str(e)}")
                print(f"Dados que causaram o erro: {dados}")
                raise
            
        elif tabela == "tabela_referencia":
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
        
        return response
    except Exception as e:
        print(f"❌ Erro ao salvar no banco (tabela {tabela}): {str(e)}")
        print(f"📄 Dados que tentamos salvar: {dados}")
        print("Stacktrace completo:")
        traceback.print_exc()
        raise

def get_mes_referencia():
    response = supabase.table('tabela_referencia').select('*').execute()
    mes_referencia = response.data
    return [{"Value": mes_referencia['codigo'], "Label": mes_referencia['mes']} for mes_referencia in mes_referencia]

@retry_on_connection_error()
def get_marcas():
    try:
        response = supabase.table('marcas').select('codigo', 'nome').execute()
        marcas = response.data
        return [{"Value": marca['codigo'], "Label": marca['nome']} for marca in marcas]
    except Exception as e:
        print(f"Erro ao buscar marcas: {e}")
        return []

@retry_on_connection_error()
def get_modelos_by_marca(marca_id):
    try:
        response = supabase.table('modelos').select('codigo', 'nome').eq('marca_id', marca_id).execute()
        modelos = response.data
        return [{"Value": modelo['codigo'], "Label": modelo['nome']} for modelo in modelos]
    except Exception as e:
        print(f"Erro ao buscar modelos da marca {marca_id}: {e}")
        return []

@retry_on_connection_error()
def get_anos_by_modelo(modelo_id):
    response = supabase.table('anos_modelo').select('id', 'codigo', 'descricao').eq('modelo_id', modelo_id).execute()
    anos = response.data
    return [{"id": ano['id'], "codigo": ano['codigo'], "Label": ano['descricao']} for ano in anos]

def clear_console():
    # Limpa a tela do console (funciona em Windows e Unix)
    os.system('cls' if os.name == 'nt' else 'clear')

@retry_on_connection_error()
def verificar_completude_dados():
    try:
        print("\n=== Verificando completude dos dados ===")
        
        # Consulta contagens
        stats = supabase.rpc('get_table_stats').execute()
        if not stats.data:
            print("Erro ao obter estatísticas. Iniciando do começo.")
            return 1
        
        counts = stats.data[0]
        print("\nDados encontrados no banco:")
        print(f"- Marcas: {counts['marcas_count']}")
        print(f"- Modelos: {counts['modelos_count']}")
        print(f"- Anos: {counts['anos_count']}")
        
        # Verifica integridade
        if counts['marcas_count'] == 0:
            print("\nNenhuma marca encontrada. Iniciando da etapa 1.")
            return 1
            
        # Verifica se há modelos para todas as marcas
        modelos_por_marca = supabase.rpc('check_marcas_sem_modelos').execute()
        if modelos_por_marca.data:
            print("\nEncontradas marcas sem modelos:")
            for marca in modelos_por_marca.data[:5]:  # Mostra até 5 exemplos
                print(f"- {marca['nome']}")
            print("\nIniciando da etapa 3 (modelos).")
            return 3
            
        # Verifica se há anos para todos os modelos
        modelos_sem_anos = supabase.rpc('check_modelos_sem_anos').execute()
        if modelos_sem_anos.data:
            print("\nEncontrados modelos sem anos:")
            for modelo in modelos_sem_anos.data[:5]:  # Mostra até 5 exemplos
                print(f"- {modelo['marca_nome']} - {modelo['modelo_nome']}")
            print("\nIniciando da etapa 4 (anos).")
            return 4
            
        print("\nTodos os dados básicos estão completos!")
        print("Iniciando da etapa 5 (preços).")
        return 5
        
    except Exception as e:
        print(f"Erro ao verificar dados: {e}")
        print("Iniciando do começo por segurança.")
        return 1

def aplicar_limite(lista, tipo):
    """Aplica limite na lista se estiver em modo amostragem"""
    if MODO_AMOSTRAGEM and tipo in LIMITES:
        return lista[:LIMITES[tipo]]
    return lista

async def rodar_scraping():
    print(f"\n=== Iniciando processo de scraping {'(AMOSTRAGEM)' if MODO_AMOSTRAGEM else ''} ===")
    
    def update_progress(etapa, detalhe="", progresso=""):
        clear_console()
        print(f"=== Processo de Scraping em Andamento {'(AMOSTRAGEM)' if MODO_AMOSTRAGEM else ''} ===")
        print(f"Etapa atual: {etapa}/5")
        if detalhe:
            print(f"Processando: {detalhe}")
        if progresso:
            print(f"Progresso: {progresso}")
        print("\nEstatísticas de requisições:")
        print(rate_tester.get_stats())
    
    etapa_inicial = verificar_completude_dados()
    
    if etapa_inicial == 1:
        update_progress("1", "Obtendo referência")
        codigo_tabela = await obter_tabela_referencia()
        salvar_no_banco("tabela_referencia", codigo_tabela)
        codigo_tabela = codigo_tabela['Codigo']
        
        update_progress("2", "Obtendo marcas")
        marcas = aplicar_limite(await obter_marcas(codigo_tabela), 'marcas')
        total_marcas = len(marcas)
        for i, marca in enumerate(marcas, 1):
            update_progress("2", f"Marca: {marca['Label']}", f"{i}/{total_marcas}")
            salvar_no_banco("marcas", marca)

    elif etapa_inicial == 3:
        update_progress("3", "Obtendo modelos")
        codigo_tabela = (await obter_tabela_referencia())['Codigo']
        marcas = aplicar_limite(get_marcas(), 'marcas')
        total_marcas = len(marcas)
        
        for i, marca in enumerate(marcas, 1):
            modelos = aplicar_limite(await obter_modelos(codigo_tabela, marca["Value"]), 'modelos')
            total_modelos = len(modelos)
            
            for j, modelo in enumerate(modelos, 1):
                update_progress("3", 
                              f"Marca: {marca['Label']} ({i}/{total_marcas})",
                              f"Modelo: {modelo['Label']} ({j}/{total_modelos})")
                modelo["marca_id"] = marca["Value"]
                salvar_no_banco("modelos", modelo)

    elif etapa_inicial == 4:
        update_progress("4", "Obtendo anos dos modelos")
        codigo_tabela = (await obter_tabela_referencia())['Codigo']
        marcas = aplicar_limite(get_marcas(), 'marcas')
        total_marcas = len(marcas)
        
        for i, marca in enumerate(marcas, 1):
            modelos = aplicar_limite(get_modelos_by_marca(marca["Value"]), 'modelos')
            total_modelos = len(modelos)
            
            for j, modelo in enumerate(modelos, 1):
                anos_modelo = aplicar_limite(
                    await obter_anos_modelo(codigo_tabela, marca["Value"], modelo["Value"]), 
                    'anos'
                )
                total_anos = len(anos_modelo)
                
                for k, ano in enumerate(anos_modelo, 1):
                    update_progress("4",
                                  f"Marca: {marca['Label']} ({i}/{total_marcas})",
                                  f"Modelo: {modelo['Label']} ({j}/{total_modelos}) - Ano: {ano['Label']} ({k}/{total_anos})")
                    ano["modelo_id"] = modelo["Value"]
                    salvar_no_banco("anos_modelo", ano)

    else:
        update_progress("5", "Obtendo valores dos veículos")
        codigo_tabela = (await obter_tabela_referencia())['Codigo']
        meses = aplicar_limite(get_mes_referencia(), 'meses')
        marcas = aplicar_limite(get_marcas(), 'marcas')
        
        total_combinacoes = 0
        todas_combinacoes = []
        
        def update_combination_progress(marca_info, modelo_info="", anos_info="", total_info=""):
            clear_console()
            print("\n=== Gerando combinações ===")
            print(f"Modo amostragem: {'Ativo' if MODO_AMOSTRAGEM else 'Inativo'}")
            print(f"\n📍 Marca: {marca_info}")
            if modelo_info:
                print(f"   ↳ Modelo: {modelo_info}")
            if anos_info:
                print(f"      ↳ {anos_info}")
            if total_info:
                print(f"\nProgresso: {total_info}")
        
        for i, marca in enumerate(marcas, 1):
            update_combination_progress(
                f"{marca['Label']} ({i}/{len(marcas)})"
            )
            
            modelos = aplicar_limite(get_modelos_by_marca(marca["Value"]), 'modelos')
            
            for j, modelo in enumerate(modelos, 1):
                update_combination_progress(
                    f"{marca['Label']} ({i}/{len(marcas)})",
                    f"{modelo['Label']} ({j}/{len(modelos)})"
                )
                
                anos = aplicar_limite(get_anos_by_modelo(modelo["Value"]), 'anos')
                combinacoes_modelo = 0
                
                for mes in meses:
                    for ano in anos:
                        todas_combinacoes.append({
                            'mes': mes,
                            'marca': marca,
                            'modelo': modelo,
                            'ano': ano
                        })
                        combinacoes_modelo += 1
                        total_combinacoes += 1
                        
                update_combination_progress(
                    f"{marca['Label']} ({i}/{len(marcas)})",
                    f"{modelo['Label']} ({j}/{len(modelos)})",
                    f"Anos processados: {len(anos)}",
                    f"Total de combinações: {total_combinacoes}"
                )

        clear_console()
        print("\n=== Geração de combinações finalizada ===")
        print(f"Total final de combinações: {total_combinacoes}")
        print(f"Total de marcas processadas: {len(marcas)}")
        print(f"Modo amostragem: {'Ativo' if MODO_AMOSTRAGEM else 'Inativo'}")

        total = len(todas_combinacoes)
        print(f"\nTotal de combinações a processar: {total}")

        for idx, combo in enumerate(todas_combinacoes, 1):
            update_progress("5",
                          f"Progresso: {idx}/{total}",
                          f"Mês: {combo['mes']['Label']} - Marca: {combo['marca']['Label']} - Modelo: {combo['modelo']['Label']} - Ano: {combo['ano']['Label']}")
            
            valor = await obter_valor_veiculo(
                codigo_tabela,
                combo['marca']["Value"],
                combo['modelo']["Value"],
                combo['ano']
            )
            
            if valor:
                salvar_no_banco("veiculos", {
                    'modelo_id': combo['modelo']["Value"],
                    'ano_id': combo['ano']["id"],
                    'mes_referencia_id': combo['mes']["Value"],
                    'codigo_fipe': valor["CodigoFipe"],
                    'combustivel': valor["Combustivel"],
                    'preco': float(valor["Valor"].replace("R$ ", "").replace(".", "").replace(",", ".")),
                })

    clear_console()
    print(f"\n\n=== Processo de scraping {'(AMOSTRAGEM)' if MODO_AMOSTRAGEM else ''} finalizado com sucesso! ===")

def log_error(e, context=""):
    """Função auxiliar para logging detalhado de erros"""
    print(f"\n❌ Erro {context}:")
    print(f"- Tipo: {type(e).__name__}")
    print(f"- Mensagem: {str(e)}")
    print("- Traceback:")
    print(traceback.format_exc())

if __name__ == "__main__":
    asyncio.run(rodar_scraping())
