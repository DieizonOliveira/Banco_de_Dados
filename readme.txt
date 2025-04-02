Teste Banco de Dados

Descrição

Este projeto tem como objetivo baixar, processar e armazenar dados financeiros de operadoras de planos de saúde utilizando 
Python e MySQL. Os dados são extraídos do site da ANS (Agência Nacional de Saúde Suplementar) e importados para um banco de 
dados MySQL para análise.

Estrutura do Projeto

O projeto é composto por dois arquivos principais:

TesteBD.py: Responsável por baixar os arquivos, criar o banco de dados e importar os dados para tabelas no MySQL.

Consultas.py: Realiza consultas SQL sobre os dados armazenados e apresenta os resultados de forma estruturada.

Dependências

Antes de executar o projeto, instale as dependências necessárias utilizando o seguinte comando:

pip install -r requirements.txt

Como Executar

1. Configurar o Banco de Dados

Certifique-se de que o MySQL está instalado e rodando na máquina. Atualize a senha do banco de dados nas funções conectar_db() e criar_banco() dentro de TesteBD.py, caso necessário.

2. Executar o Script de Processamento

Para baixar e importar os dados para o MySQL, execute:

python TesteBD.py

Esse script irá:

Criar o banco de dados estagio_dieizon.

Baixar arquivos CSV e ZIP do site da ANS.

Criar tabelas a partir dos arquivos baixados.

Importar os dados para o banco de dados.

3. Executar Consultas

Para realizar consultas sobre os dados armazenados, execute:

python Consultas.py

Esse script executará duas consultas principais:

Últimos 3 meses: Obtém os dados mais recentes sobre despesas das operadoras.

Último ano: Obtém os dados de todo o ano de 2024.

Os resultados serão exibidos em formato de tabela no terminal.