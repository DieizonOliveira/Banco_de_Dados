import mysql.connector
from mysql.connector import Error
from datetime import datetime, timedelta
import pymysql
from pymysql import Error
from prettytable import PrettyTable

# Função para conectar ao banco de dados MySQL
def conectar_db():
    try:
        conn = pymysql.connect(
            host="localhost",
            user="root",
            password="senha123",  # Substitua com a senha do seu MySQL
            database="estagio_dieizon",  # Substitua com o nome do seu banco de dados
        )
        print("Conexão bem-sucedida ao banco de dados.")
        return conn
    except Error as e:
        print(f"Erro ao conectar ao MySQL: {e}")
        return None

# Função para exibir os resultados como uma tabela
def exibir_tabela(resultado):
    # Criando a tabela usando PrettyTable
    tabela = PrettyTable()
    
    # Definindo os nomes das colunas
    tabela.field_names = ["Registro_ANS", "CNPJ", "Nome_Fantasia", "Razao_Social", "Descricao", "Total_Despesas"]
    
    # Adicionando as linhas da consulta à tabela
    for row in resultado:
        tabela.add_row(row)
    
    # Exibindo a tabela
    print(tabela)

# Função para executar a consulta
def executar_consulta(consulta):
    conn = conectar_db()
    if conn is not None:
        cursor = conn.cursor()
        try:
            cursor.execute(consulta)
            resultado = cursor.fetchall()
            exibir_tabela(resultado)  # Chama a função para exibir a tabela
        except Error as e:
            print(f"Erro ao executar a consulta: {e}")
        finally:
            cursor.close()
            conn.close()

# Função para consulta dos últimos 3 meses
def consulta_3_meses():
    consulta = """
    SELECT 
        o.Registro_ANS, 
        o.CNPJ, 
        o.Nome_Fantasia, 
        o.Razao_Social,
        d.DESCRICAO,
        SUM(CAST(COALESCE(NULLIF(d.VL_SALDO_FINAL, ''), 0) AS DECIMAL(10, 2))) AS Total_Despesas
    FROM (
        SELECT * FROM demonstrativos_2023 
        UNION ALL 
        SELECT * FROM demonstrativos_2024
    ) d
    JOIN operadoras o ON d.REG_ANS = o.Registro_ANS
    WHERE d.DESCRICAO LIKE '%EVENTOS/ SINISTROS CONHECIDOS OU AVISADOS%'
    AND (
        -- Comparação fixa com a data de corte 01/10/2024
        IF(
            d.DATA REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$',
            STR_TO_DATE(d.DATA, '%d/%m/%Y'),
            STR_TO_DATE(d.DATA, '%Y-%m-%d')
        ) >= '2024-10-01'
    )
    GROUP BY o.Registro_ANS, o.CNPJ, o.Nome_Fantasia, o.Razao_Social, d.DESCRICAO
    ORDER BY Total_Despesas DESC
    LIMIT 10;
    """
    return consulta

# Função para consulta de 1 ano (dados de 2024)
def consulta_1_ano():
    consulta = """
    SELECT 
        o.Registro_ANS, 
        o.CNPJ, 
        o.Nome_Fantasia, 
        o.Razao_Social,
        d.DESCRICAO,
        SUM(CAST(COALESCE(NULLIF(d.VL_SALDO_FINAL, ''), 0) AS DECIMAL(10, 2))) AS Total_Despesas
    FROM (
        SELECT * FROM demonstrativos_2023 
        UNION ALL 
        SELECT * FROM demonstrativos_2024
    ) d
    JOIN operadoras o ON d.REG_ANS = o.Registro_ANS
    WHERE d.DESCRICAO LIKE '%EVENTOS/ SINISTROS CONHECIDOS OU AVISADOS%'
    AND (
        -- Filtra apenas dados de 2024 (último ano antes de 31/12/2024)
        IF(
            d.DATA REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$',
            STR_TO_DATE(d.DATA, '%d/%m/%Y'),
            STR_TO_DATE(d.DATA, '%Y-%m-%d')
        ) BETWEEN '2024-01-01' AND '2024-12-31'
    )
    GROUP BY o.Registro_ANS, o.CNPJ, o.Nome_Fantasia, o.Razao_Social, d.DESCRICAO
    ORDER BY Total_Despesas DESC
    LIMIT 10;
    """
    return consulta

# Função principal para executar as consultas
def main():
    # Consulta 1: Dados dos últimos 3 meses
    consulta = consulta_3_meses()
    print(f"Consultando dados dos últimos 3 meses (a partir de 01/10/2024):")
    executar_consulta(consulta)
    
    # Consulta 2: Dados de 2024 (ano completo)
    consulta = consulta_1_ano()
    print(f"\nConsultando dados de 2024 (ano completo):")
    executar_consulta(consulta)

if __name__ == "__main__":
    main()


