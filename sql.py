import sqlalchemy
import psycopg2 as psg
from environs import Env
import pandas as pd

env = Env()
env.read_env()

class BancoSistransito:

    def __init__(self):
        self.__connection = env('SISTRANSITO_DATABASE')

    def __get_connection(self):
        engine = sqlalchemy.create_engine(self.__connection)
        return engine

    def def_temp_table(self):
        engine = self.__get_connection()
        self.connection = engine.connect()
        self.connection.execute('DROP TABLE IF EXISTS sistransito_rec_temp')
        self.connection.execute('CREATE TEMPORARY TABLE sistransito_rec_temp ( pk int NOT NULL, delito varchar NULL, situacao_ocorrencia varchar NULL, placa_veiculo varchar NULL, uf_veiculo varchar NULL, chassi_veiculo varchar NULL, marca_modelo varchar NULL, cor varchar NULL, ano_fabricacao int4 NULL, ano_modelo int4 NULL, tipo_veiculo varchar NULL, tipo_veiculo_1 varchar NULL, tipo_veiculo_2 varchar NULL, categoria varchar NULL, uf_bop varchar NULL, n_bop_recuperacao varchar NULL, n_bop_declaracao varchar NULL, data_registro date NULL, hora_registro time NULL, data_fato date NULL, mes_registro int4 NULL, mes_fato int4 NULL, ano_registro int4 NULL, ano_fato_siac int4 NULL, ano_fato int4 NULL, dia_semana varchar NULL, hora_fato time NULL, faixa_hora varchar NULL, tempo_recuperacao varchar NULL, endereco_recuperacao varchar NULL, local_recuperacao varchar NULL, local_recuperacao_siac varchar NULL, uf_recuperacao varchar NULL, nmbairro varchar NULL, regiao varchar NULL, risp varchar NULL, CONSTRAINT sistransito_recuperacao_pk PRIMARY KEY (pk));')

    def set_temp_data(self, df):
        df.to_sql("sistransito_rec_temp",self.connection, index=False,if_exists="append")

    def merge_sistransito(self):
        self.connection.execute("""insert into sistransito_recuperacao (delito , situacao_ocorrencia , placa_veiculo , uf_veiculo , chassi_veiculo , marca_modelo , cor , ano_fabricacao , ano_modelo , tipo_veiculo , tipo_veiculo_1 , tipo_veiculo_2 , categoria , uf_bop , n_bop_recuperacao , n_bop_declaracao , data_registro , hora_registro , data_fato , mes_registro , mes_fato , ano_registro , ano_fato_siac , ano_fato , dia_semana , hora_fato , faixa_hora , tempo_recuperacao , endereco_recuperacao , local_recuperacao , local_recuperacao_siac , uf_recuperacao , nmbairro , regiao , risp ) select delito , situacao_ocorrencia , placa_veiculo , uf_veiculo , chassi_veiculo , marca_modelo , cor , ano_fabricacao , ano_modelo , tipo_veiculo , tipo_veiculo_1 , tipo_veiculo_2 , categoria , uf_bop , n_bop_recuperacao , n_bop_declaracao , data_registro , hora_registro , data_fato , mes_registro , mes_fato , ano_registro , ano_fato_siac , ano_fato , dia_semana , hora_fato , faixa_hora , tempo_recuperacao , endereco_recuperacao , local_recuperacao , local_recuperacao_siac , uf_recuperacao , nmbairro , regiao , risp from sistransito_rec_temp""")
