# Análise de Criminalidade por Estado

## Importações e Configurações Iniciais


# %%
import pandas as pd
import unicodedata
import seaborn as sns
import matplotlib.pyplot as plt


## Definição da Função de Análise


# %%
def analise_criminalidade(uf, ano, populacao_minima=100000):
    """
    Função completa de análise de criminalidade para um estado específico.
    
    Parâmetros:
    -----------
    uf : str
        Unidade federativa para análise
    ano : int
        Ano para análise
    populacao_minima : int, opcional (padrão=100000)
        População mínima para filtrar os municípios
    
    Retorna:
    --------
    dict
        Dicionário contendo DataFrames e visualizações
    """
    # Funções auxiliares
    def remover_acentos(texto):
        """Remove acentos de uma string."""
        normalizado = unicodedata.normalize('NFKD', str(texto))
        sem_acentos = ''.join([c for c in normalizado if not unicodedata.combining(c)])
        return sem_acentos

    def formatar_municipio(municipio):
        """Formata o nome do município."""
        if pd.isna(municipio):
            return 'NÃO INFORMADO'
        
        municipio_str = str(municipio).strip()
        
        if municipio_str.upper() == 'NÃO INFORMADO':
            return 'NÃO INFORMADO'
        
        return municipio_str.lower()

    # Carregamento de dados
    df = pd.read_parquet(r'C:\Users\daviw\OneDrive\Projetos\Python\Segurança Pública\data\processed\df_unificado.parquet')
    
    df_populacao = pd.read_excel(
        r'C:\Users\daviw\OneDrive\Projetos\Python\Segurança Pública\data\raw\estimativa_dou_2024.xls', 
        sheet_name='MUNICÍPIOS', 
        skiprows=1, 
        skipfooter=1
    )
    df_populacao['NOME DO MUNICÍPIO'] = df_populacao['NOME DO MUNICÍPIO'].astype(str).apply(remover_acentos)
    df_populacao['chave'] = df_populacao['UF'] + df_populacao['NOME DO MUNICÍPIO'].str.lower()
    
    df_obitos_arma_fogo = pd.read_csv(r'C:\Users\daviw\OneDrive\Projetos\Python\Segurança Pública\data\processed\obitos_datasus.csv')

    # Preparação dos dados
    df['municipio'] = df['municipio'].apply(formatar_municipio)
    df['municipio'] = df['municipio'].apply(remover_acentos)
    df['chave_uf'] = df['uf'] + df['municipio']

    # Filtragem
    df_filtrado = df[
        (df['uf'] == uf) & 
        (df['ano'] == ano) & 
        (df['municipio'] != 'NAO INFORMADO')
    ].reset_index(drop=True)

    # Adicionar população
    df_filtrado['populacao'] = df_filtrado['chave_uf'].map(df_populacao.set_index('chave')['POPULAÇÃO ESTIMADA'])

    # Filtrar por população mínima
    df_filtrado = df_filtrado[df_filtrado['populacao'] >= populacao_minima].reset_index(drop=True)

    # Adicionar óbitos
    df_filtrado['obitos'] = df_filtrado['chave_uf'].map(df_obitos_arma_fogo.set_index('chave_uf')['obitos'])

    # Criar chave de agrupamento
    df_filtrado['chave_agrupamento'] = (
        df_filtrado['uf'] + "," + 
        df_filtrado['municipio'] + "," + 
        df_filtrado['evento'] + "," + 
        df_filtrado['ano'].astype(str) + "," + 
        df_filtrado['categoria']
    )

    # Agregação
    agrupamento_valor = df_filtrado.groupby('chave_agrupamento')['valor'].sum().reset_index()
    agrupamento_obitos = df_filtrado.groupby('chave_agrupamento')['obitos'].min().reset_index()
    agrupamento_populacao = df_filtrado.groupby('chave_agrupamento')['populacao'].min().reset_index()

    # Mesclar dataframes
    df_agrupado = agrupamento_valor.merge(agrupamento_obitos, on="chave_agrupamento")
    df_agrupado = df_agrupado.merge(agrupamento_populacao, on="chave_agrupamento")

    # Calcular taxas por 100 mil habitantes
    df_agrupado['tx_100_mil_hab'] = df_agrupado['valor'] / df_agrupado['populacao'] * 100000
    df_agrupado['obitos_100_mil_hab'] = df_agrupado['obitos'] / df_agrupado['populacao'] * 100000

    # Expandir chave de agrupamento
    df_expandido = df_agrupado.copy()
    df_expandido[['estado', 'municipio', 'crime', 'ano', 'categoria']] = df_expandido['chave_agrupamento'].str.split(',', expand=True)
    df_expandido = df_expandido.drop(columns=['chave_agrupamento'])
    df_expandido = df_expandido[['estado', 'municipio', 'crime', 'ano', 'categoria', 'valor', 'obitos', 'populacao', 'tx_100_mil_hab', 'obitos_100_mil_hab']]

    # Calcular médias e taxas por município
    vars_selecionadas = {}
    tent_hom = df_expandido[df_expandido['categoria'] == 'Tentativa de homicídio'].groupby('municipio')['tx_100_mil_hab'].sum().sort_values(ascending=True)
    morte_transito = df_expandido[df_expandido['categoria'] == 'Morte no trânsito ou em decorrência dele (exceto homicídio doloso)'].groupby('municipio')['tx_100_mil_hab'].sum().sort_values(ascending=True)
    Crimes_Contra_a_Vida = df_expandido[df_expandido['categoria'] == 'Crimes Contra a Vida'].groupby('municipio')['tx_100_mil_hab'].sum().sort_values(ascending=True)
    mortes_por_arma_de_fogo = df_expandido.groupby('municipio')['obitos_100_mil_hab'].mean().sort_values(ascending=True)

    vars_selecionadas['Tentativa de homicídio'] = tent_hom
    vars_selecionadas['Morte no trânsito ou em decorrência dele (exceto homicídio doloso)'] = morte_transito
    vars_selecionadas['Crimes Contra a Vida'] = Crimes_Contra_a_Vida
    vars_selecionadas['mortes_por_arma_de_fogo'] = mortes_por_arma_de_fogo

    df_variaveis_selecionadas = pd.DataFrame(vars_selecionadas)

    # Visualização
    plt.figure(figsize=(12, 8))
    ax = sns.scatterplot(
        data=df_variaveis_selecionadas,
        x='Tentativa de homicídio',
        y='mortes_por_arma_de_fogo',
        hue='Crimes Contra a Vida'
    )

    # Linhas de quadrantes
    media_x = df_variaveis_selecionadas['Tentativa de homicídio'].quantile(0.5)
    media_y = df_variaveis_selecionadas['mortes_por_arma_de_fogo'].quantile(0.5)
    ax.axvline(media_x, color='gray', linestyle='--')
    ax.axhline(media_y, color='gray', linestyle='--')

    # Anotações de municípios
    for idx, row in df_variaveis_selecionadas.iterrows():
        ax.annotate(
            idx,
            (row['Tentativa de homicídio'], row['mortes_por_arma_de_fogo']),
            textcoords="offset points",
            xytext=(5, 5),
            ha='left'
        )

    plt.title(f'Crimes em Municípios de {uf} - Ano {ano}')
    plt.xlabel('Tentativa de homicídio')
    plt.ylabel('Mortes por Arma de Fogo')
    plt.legend(title='Crimes Contra a Vida')
    plt.tight_layout()
    plt.show()

    # Retornar resultados
    return {
        'dados_expandidos': df_expandido,
        'variaveis_selecionadas': df_variaveis_selecionadas,
        'correlacao': df_variaveis_selecionadas.corr()
    }


## Análise de Criminalidade para Rio de Janeiro em 2023


# %%
# Executar análise para Rio de Janeiro
resultado_RJ = analise_criminalidade('RJ', 2023)


## Visualização dos Dados Expandidos


# %%
# Mostrar primeiras linhas dos dados expandidos
resultado_RJ['dados_expandidos'].head()


## Variáveis Selecionadas


# %%
# Mostrar variáveis selecionadas
resultado_RJ['variaveis_selecionadas']


## Matriz de Correlação


# %%
# Exibir matriz de correlação
resultado_RJ['correlacao']


## Análise para Outros Estados


# %%
# Exemplo de análise para São Paulo
resultado_SP = analise_criminalidade('SP', 2023, populacao_minima=500000)
