# =>ALTERE A SENHA DO BANCO DE DADOS NAS FUNCOES conectar_db e criar_banco.<=

import os
import requests
import zipfile
import pymysql
import csv
import time
import itertools
from io import BytesIO
from bs4 import BeautifulSoup
from threading import Thread

def animacao(mensagem):
    for c in itertools.cycle(['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏']):
        print(f"\r{mensagem} {c}", end="", flush=True)
        time.sleep(0.1)

def conectar_db():
    conn = pymysql.connect(
        host="localhost",
        user="root",
        password="senha123",
        database="estagio_dieizon",
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
    return conn

def criar_banco():
    conn = pymysql.connect(
        host="localhost",
        user="root",
        password="senha123",
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
    cursor = conn.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS estagio_dieizon;")
    conn.commit()
    print("✅ Banco de dados 'estagio_dieizon' verificado/criado com sucesso.")
    cursor.close()
    conn.close()

def baixar_arquivos():
    print("📥 Baixando arquivos...")
    url_demonstrativos = 'https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/'
    pasta_downloads = 'downloads'
    os.makedirs(pasta_downloads, exist_ok=True)
    
    def baixar_e_extrair_zip(url_pasta, ano):
        response = requests.get(url_pasta + ano)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            zip_links = [a['href'] for a in soup.find_all('a', href=True) if a['href'].endswith('.zip')]
            for link in zip_links:
                zip_url = url_pasta + ano + '/' + link
                print(f"📂 Baixando {zip_url}")
                zip_response = requests.get(zip_url)
                with zipfile.ZipFile(BytesIO(zip_response.content)) as zip_ref:
                    zip_ref.extractall(pasta_downloads)
                print(f"✅ {link} extraído com sucesso!")
        else:
            print(f"❌ Falha ao acessar a pasta {ano}")
    
    for ano in ['2024', '2023']:
        baixar_e_extrair_zip(url_demonstrativos, ano)
    
    url_operadoras = 'https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/Relatorio_cadop.csv'
    response = requests.get(url_operadoras)
    with open(os.path.join(pasta_downloads, 'Relatorio_cadop.csv'), 'wb') as file:
        file.write(response.content)
    print("✅ Arquivos CSV baixados com sucesso!")

def criar_tabela_a_partir_do_csv(csv_filename, tabela_nome):
    print(f"🛠 Criando ou atualizando tabela `{tabela_nome}`...")
    conn = conectar_db()
    cursor = conn.cursor()
    with open(csv_filename, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=';')
        header = next(reader)
    
    cursor.execute(f"SHOW TABLES LIKE '{tabela_nome}'")
    tabela_existe = cursor.fetchone()
    
    if not tabela_existe:
        criar_query = f"CREATE TABLE {tabela_nome} ("
        for coluna in header:
            coluna = coluna.strip().replace(' ', '_')
            criar_query += f"{coluna} VARCHAR(255), "
        criar_query = criar_query.rstrip(', ') + ");"
        cursor.execute(criar_query)
        conn.commit()
        print(f"✅ Tabela `{tabela_nome}` criada com sucesso!")
    else:
        print(f"🔄 Tabela `{tabela_nome}` já existe. Atualizando dados...")
    
    cursor.close()
    conn.close()

def importar_dados_para_tabela(csv_filename, tabela_nome):
    print(f"📊 Importando dados para `{tabela_nome}`...")
    conn = conectar_db()
    cursor = conn.cursor()
    with open(csv_filename, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=';')
        header = next(reader)
        for row in reader:
            row = [None if value == '' else value for value in row]
            placeholders = ', '.join(['%s'] * len(row))
            query = f"INSERT INTO {tabela_nome} ({', '.join(header)}) VALUES ({placeholders})"
            try:
                cursor.execute(query, row)
            except pymysql.MySQLError as err:
                print(f"❌ Erro ao inserir dados na tabela `{tabela_nome}`: {err}")
                print(f"⚠️ Dados problemáticos: {row}")
    conn.commit()
    print(f"✅ Dados importados para `{tabela_nome}` com sucesso!")
    cursor.close()
    conn.close()

def processar_csv_e_importar_dados():
    pasta_downloads = 'downloads'
    arquivo_operadoras = os.path.join(pasta_downloads, 'Relatorio_cadop.csv')
    criar_tabela_a_partir_do_csv(arquivo_operadoras, 'operadoras')
    importar_dados_para_tabela(arquivo_operadoras, 'operadoras')
    for ano in ['2024', '2023']:
        arquivos_demonstrativos = [
            os.path.join(pasta_downloads, arquivo)
            for arquivo in os.listdir(pasta_downloads)
            if arquivo.endswith('.csv') and ano in arquivo
        ]
        for arquivo in arquivos_demonstrativos:
            tabela_nome = f"demonstrativos_{ano}"
            if not os.path.exists(arquivo):
                print(f"⚠️ Arquivo `{arquivo}` não encontrado. Pulando...")
                continue
            criar_tabela_a_partir_do_csv(arquivo, tabela_nome)
            importar_dados_para_tabela(arquivo, tabela_nome)
            print(f"🔄 Atualizando tabela `{tabela_nome}` com novos dados...")

def executar():
    criar_banco()
    t = Thread(target=animacao, args=("⏳ Processando...",))
    t.daemon = True
    t.start()
    baixar_arquivos()
    processar_csv_e_importar_dados()
    print("✅ Processo concluído com sucesso!")

if __name__ == '__main__':
    executar()

