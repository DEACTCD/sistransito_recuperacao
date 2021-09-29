# -*- coding: utf-8 -*-
import pandas as pd
import os
import datetime
import sqlalchemy
import openpyxl

class SistransitoRecuperacao:

    def __init__(self):
        self.__url = os.path.join(os.getcwd(),"DEPOSITE_ARQUIVO")

    def get_files(self):
        list_of_files = os.listdir(self.__url)

        list_of_tuples = []

        for file in list_of_files:
            name = file.split('.')[0]
            tuple_files = (file,name)
            list_of_tuples.append(tuple_files)

        return list_of_tuples

    def __get_data(self, file,delict):
        datan = os.path.join(self.__url,file)
        df = pd.read_excel(io = datan, sheet_name="Planilha1",engine='openpyxl')
        df = df.applymap(lambda s: s.upper() if type(s) == str else s)
        df["DELITO"] = delict
        return df

    def get_dataFrames(self):
        files = self.get_files()
        dfs = []
        for file in files:
            df = self.__get_data(file[0],file[1])
            dfs.append(df)
        dft = pd.concat(dfs)
        dft = dft.reset_index()
        return dft

class SupportRules:

    def __init__(self,df):
        self.__df = df
        self.__regiao = {"ABAETETUBA":"TOCANTINS" , "ABEL FIGUEIREDO":"CARAJÁS" , "ACARA":"TOCANTINS" , "AFUA":"MARAJÓ OCIDENTAL" , "AGUA AZUL DO NORTE":"ALTO XINGÚ" , "ALENQUER":"BAIXO AMAZONAS" , "ALMEIRIM":"BAIXO AMAZONAS" , "ALTAMIRA":"XINGÚ" , "ANAJAS":"MARAJÓ OCIDENTAL" , "ANANINDEUA":"METROPOLITANA" , "ANAPU":"XINGÚ" , "AUGUSTO CORREA":"CAETÉ" , "AURORA DO PARA":"CAPIM" , "AVEIRO":"TAPAJÓS" , "BAGRE":"MARAJÓ OCIDENTAL" , "BAIAO":"TOCANTINS" , "BANNACH":"ALTO XINGÚ" , "BARCARENA":"TOCANTINS" , "BELEM":"CAPITAL" , "BELTERRA":"BAIXO AMAZONAS" , "BENEVIDES":"METROPOLITANA" , "BOM JESUS DO TOCANTINS":"CARAJÁS" , "BONITO":"CAETÉ" , "BRAGANCA":"CAETÉ" , "BRASIL NOVO":"XINGÚ" , "BREJO GRANDE DO ARAGUAIA":"CARAJÁS" , "BREU BRANCO":"LAGO DO TUCURUÍ" , "BREVES":"MARAJÓ OCIDENTAL" , "BUJARU":"GUAMÁ" , "CACHOEIRA DO ARARI":"MARAJÓ ORIENTAL" , "CACHOEIRA DO PIRIA":"CAETÉ" , "CAMETA":"TOCANTINS" , "CANAA DOS CARAJAS":"CARAJÁS" , "CAPANEMA":"CAETÉ" , "CAPITAO POCO":"CAETÉ" , "CASTANHAL":"GUAMÁ" , "CASTELO DOS SONHOS":"TAPAJÓS" , "CHAVES":"MARAJÓ OCIDENTAL" , "COLARES":"GUAMÁ" , "CONCEICAO DO ARAGUAIA":"ARAGUAIA" , "CONCORDIA DO PARA":"GUAMÁ" , "CUMARU DO NORTE":"ARAGUAIA" , "CURIONOPOLIS":"CARAJÁS" , "CURRALINHO":"MARAJÓ OCIDENTAL" , "CURUA":"BAIXO AMAZONAS" , "CURUCA":"GUAMÁ" , "DOM ELISEU":"CAPIM" , "ELDORADO DOS CARAJAS":"CARAJÁS" , "FARO":"BAIXO AMAZONAS" , "FLORESTA DO ARAGUAIA":"ARAGUAIA" , "GARRAFAO DO NORTE":"CAETÉ" , "GOIANESIA DO PARA":"LAGO DO TUCURUÍ" , "GURUPA":"MARAJÓ OCIDENTAL" , "IGARAPE-ACU":"GUAMÁ" , "IGARAPE-MIRI":"TOCANTINS" , "INHANGAPI":"GUAMÁ" , "IPIXUNA DO PARA":"CAPIM" , "IRITUIA":"GUAMÁ" , "ITAITUBA":"TAPAJÓS" , "ITUPIRANGA":"CARAJÁS" , "JACAREACANGA":"TAPAJÓS" , "JACUNDA":"LAGO DO TUCURUÍ" , "JURUTI":"BAIXO AMAZONAS" , "LIMOEIRO DO AJURU":"TOCANTINS" , "MAE DO RIO":"CAPIM" , "MAGALHAES BARATA":"GUAMÁ" , "MARABA":"CARAJÁS" , "MARACANA":"GUAMÁ" , "MARAPANIM":"GUAMÁ" , "MARITUBA":"METROPOLITANA" , "MEDICILANDIA":"XINGÚ" , "MELGACO":"MARAJÓ OCIDENTAL" , "MOCAJUBA":"TOCANTINS" , "MOJU":"TOCANTINS" , "MOJUI DOS CAMPOS":"BAIXO AMAZONAS" , "MONTE ALEGRE":"BAIXO AMAZONAS" , "MUANA":"MARAJÓ ORIENTAL" , "NOVA ESPERANCA DO PIRIA":"CAETÉ" , "NOVA IPIXUNA":"CARAJÁS" , "NOVA TIMBOTEUA":"CAETÉ" , "NOVO PROGRESSO":"TAPAJÓS" , "NOVO REPARTIMENTO":"LAGO DO TUCURUÍ" , "OBIDOS":"BAIXO AMAZONAS" , "OEIRAS DO PARA":"TOCANTINS" , "ORIXIMINA":"BAIXO AMAZONAS" , "OUREM":"CAETÉ" , "OURILANDIA DO NORTE":"ALTO XINGÚ" , "PACAJA":"LAGO DO TUCURUÍ" , "PALESTINA DO PARA":"CARAJÁS" , "PARAGOMINAS":"CAPIM" , "PARAUAPEBAS":"CARAJÁS" , "PARAUAPEBAS":"CARAJÁS" , "PAU D ARCO":"ARAGUAIA" , "PEIXE-BOI":"CAETÉ" , "PICARRA":"CARAJÁS" , "PLACAS":"TAPAJÓS" , "PONTA DE PEDRAS":"MARAJÓ ORIENTAL" , "PORTEL":"MARAJÓ OCIDENTAL" , "PORTO DE MOZ":"XINGÚ" , "PRAINHA":"BAIXO AMAZONAS" , "PRIMAVERA":"CAETÉ" , "QUATIPURU":"CAETÉ" , "REDENCAO":"ARAGUAIA" , "RIO MARIA":"ALTO XINGÚ" , "RONDON DO PARA":"CARAJÁS" , "RUROPOLIS":"TAPAJÓS" , "SALINOPOLIS":"CAETÉ" , "SALVATERRA":"MARAJÓ ORIENTAL" , "SANTA BARBARA DO PARA":"METROPOLITANA" , "SANTA CRUZ DO ARARI":"MARAJÓ ORIENTAL" , "SANTA ISABEL DO PARA":"GUAMÁ" , "SANTA LUZIA DO PARA":"CAETÉ" , "SANTA MARIA DAS BARREIRAS":"ARAGUAIA" , "SANTA MARIA DO PARA":"GUAMÁ" , "SANTANA DO ARAGUAIA":"ARAGUAIA" , "SANTAREM":"BAIXO AMAZONAS" , "SANTAREM NOVO":"CAETÉ" , "SANTO ANTONIO DO TAUA":"GUAMÁ" , "SAO CAETANO DE ODIVELAS":"GUAMÁ" , "SAO DOMINGOS DO ARAGUAIA":"CARAJÁS" , "SAO DOMINGOS DO CAPIM":"GUAMÁ" , "SAO FELIX DO XINGU":"ALTO XINGÚ" , "SAO FRANCISCO DO PARA":"GUAMÁ" , "SAO GERALDO DO ARAGUAIA":"CARAJÁS" , "SAO JOAO DA PONTA":"GUAMÁ" , "SAO JOAO DE PIRABAS":"CAETÉ" , "SAO JOAO DO ARAGUAIA":"CARAJÁS" , "SAO MIGUEL DO GUAMA":"GUAMÁ" , "SAO SEBASTIAO DA BOA VISTA":"MARAJÓ OCIDENTAL" , "SAPUCAIA":"ALTO XINGÚ" , "SENADOR JOSE PORFIRIO":"XINGÚ" , "SOURE":"MARAJÓ ORIENTAL" , "TAILANDIA":"LAGO DO TUCURUÍ" , "TERRA ALTA":"GUAMÁ" , "TERRA SANTA":"BAIXO AMAZONAS" , "TOME-ACU":"GUAMÁ" , "TRACUATEUA":"CAETÉ" , "TRAIRAO":"TAPAJÓS" , "TUCUMA":"ALTO XINGÚ" , "TUCURUI":"LAGO DO TUCURUÍ" , "ULIANOPOLIS":"CAPIM" , "URUARA":"XINGÚ" , "VIGIA":"GUAMÁ" , "VISEU":"CAETÉ" , "VITORIA DO XINGU":"XINGÚ" , "XINGUARA":"ALTO XINGÚ"}
        self.__risp = {"BELEM":"01ª RISP" , "ANANINDEUA":"02ª RISP" , "BENEVIDES":"02ª RISP" , "MARITUBA":"02ª RISP" , "SANTA BARBARA DO PARA":"02ª RISP" , "BUJARU":"03ª RISP" , "CASTANHAL":"03ª RISP" , "COLARES":"03ª RISP" , "CONCORDIA DO PARA":"03ª RISP" , "CURUCA":"03ª RISP" , "IGARAPE-ACU":"03ª RISP" , "INHANGAPI":"03ª RISP" , "IRITUIA":"03ª RISP" , "MAGALHAES BARATA":"03ª RISP" , "MARACANA":"03ª RISP" , "MARAPANIM":"03ª RISP" , "SANTA ISABEL DO PARA":"03ª RISP" , "SANTA MARIA DO PARA":"03ª RISP" , "SANTO ANTONIO DO TAUA":"03ª RISP" , "SAO CAETANO DE ODIVELAS":"03ª RISP" , "SAO DOMINGOS DO CAPIM":"03ª RISP" , "SAO FRANCISCO DO PARA":"03ª RISP" , "SAO JOAO DA PONTA":"03ª RISP" , "SAO MIGUEL DO GUAMA":"03ª RISP" , "TERRA ALTA":"03ª RISP" , "TOME-ACU":"03ª RISP" , "VIGIA":"03ª RISP" , "ABAETETUBA":"04ª RISP" , "ACARA":"04ª RISP" , "BAIAO":"04ª RISP" , "BARCARENA":"04ª RISP" , "CAMETA":"04ª RISP" , "IGARAPE-MIRI":"04ª RISP" , "LIMOEIRO DO AJURU":"04ª RISP" , "MOCAJUBA":"04ª RISP" , "MOJU":"04ª RISP" , "OEIRAS DO PARA":"04ª RISP" , "CACHOEIRA DO ARARI":"05ª RISP" , "MUANA":"05ª RISP" , "PONTA DE PEDRAS":"05ª RISP" , "SALVATERRA":"05ª RISP" , "SANTA CRUZ DO ARARI":"05ª RISP" , "SOURE":"05ª RISP" , "AUGUSTO CORREA":"06ª RISP" , "BONITO":"06ª RISP" , "BRAGANCA":"06ª RISP" , "CACHOEIRA DO PIRIA":"06ª RISP" , "CAPANEMA":"06ª RISP" , "CAPITAO POCO":"06ª RISP" , "GARRAFAO DO NORTE":"06ª RISP" , "NOVA ESPERANCA DO PIRIA":"06ª RISP" , "NOVA TIMBOTEUA":"06ª RISP" , "OUREM":"06ª RISP" , "PEIXE-BOI":"06ª RISP" , "PRIMAVERA":"06ª RISP" , "QUATIPURU":"06ª RISP" , "SALINOPOLIS":"06ª RISP" , "SANTA LUZIA DO PARA":"06ª RISP" , "SANTAREM NOVO":"06ª RISP" , "SAO JOAO DE PIRABAS":"06ª RISP" , "TRACUATEUA":"06ª RISP" , "VISEU":"06ª RISP" , "AURORA DO PARA":"07ª RISP" , "DOM ELISEU":"07ª RISP" , "IPIXUNA DO PARA":"07ª RISP" , "MAE DO RIO":"07ª RISP" , "PARAGOMINAS":"07ª RISP" , "ULIANOPOLIS":"07ª RISP" , "AFUA":"08ª RISP" , "ANAJAS":"08ª RISP" , "BAGRE":"08ª RISP" , "BREVES":"08ª RISP" , "CHAVES":"08ª RISP" , "CURRALINHO":"08ª RISP" , "GURUPA":"08ª RISP" , "MELGACO":"08ª RISP" , "PORTEL":"08ª RISP" , "SAO SEBASTIAO DA BOA VISTA":"08ª RISP" , "BREU BRANCO":"09ª RISP" , "GOIANESIA DO PARA":"09ª RISP" , "JACUNDA":"09ª RISP" , "NOVO REPARTIMENTO":"09ª RISP" , "PACAJA":"09ª RISP" , "TAILANDIA":"09ª RISP" , "TUCURUI":"09ª RISP" , "ABEL FIGUEIREDO":"10ª RISP" , "BOM JESUS DO TOCANTINS":"10ª RISP" , "BREJO GRANDE DO ARAGUAIA":"10ª RISP" , "CANAA DOS CARAJAS":"10ª RISP" , "CURIONOPOLIS":"10ª RISP" , "ELDORADO DOS CARAJAS":"10ª RISP" , "ITUPIRANGA":"10ª RISP" , "MARABA":"10ª RISP" , "NOVA IPIXUNA":"10ª RISP" , "PALESTINA DO PARA":"10ª RISP" , "PARAUAPEBAS":"10ª RISP" , "PICARRA":"10ª RISP" , "RONDON DO PARA":"10ª RISP" , "SAO DOMINGOS DO ARAGUAIA":"10ª RISP" , "SAO GERALDO DO ARAGUAIA":"10ª RISP" , "SAO JOAO DO ARAGUAIA":"10ª RISP" , "ALTAMIRA":"11ª RISP" , "ANAPU":"11ª RISP" , "BRASIL NOVO":"11ª RISP" , "MEDICILANDIA":"11ª RISP" , "PORTO DE MOZ":"11ª RISP" , "SENADOR JOSE PORFIRIO":"11ª RISP" , "URUARA":"11ª RISP" , "VITORIA DO XINGU":"11ª RISP" , "ALENQUER":"12ª RISP" , "ALMEIRIM":"12ª RISP" , "BELTERRA":"12ª RISP" , "CURUA":"12ª RISP" , "FARO":"12ª RISP" , "JURUTI":"12ª RISP" , "MOJUI DOS CAMPOS":"12ª RISP" , "MONTE ALEGRE":"12ª RISP" , "OBIDOS":"12ª RISP" , "ORIXIMINA":"12ª RISP" , "PRAINHA":"12ª RISP" , "SANTAREM":"12ª RISP" , "TERRA SANTA":"12ª RISP" , "CONCEICAO DO ARAGUAIA":"13ª RISP" , "CUMARU DO NORTE":"13ª RISP" , "FLORESTA DO ARAGUAIA":"13ª RISP" , "PAU D ARCO":"13ª RISP" , "REDENCAO":"13ª RISP" , "SANTA MARIA DAS BARREIRAS":"13ª RISP" , "SANTANA DO ARAGUAIA":"13ª RISP" , "AGUA AZUL DO NORTE":"14ª RISP" , "BANNACH":"14ª RISP" , "OURILANDIA DO NORTE":"14ª RISP" , "RIO MARIA":"14ª RISP" , "SAO FELIX DO XINGU":"14ª RISP" , "SAPUCAIA":"14ª RISP" , "TUCUMA":"14ª RISP" , "XINGUARA":"14ª RISP" , "AVEIRO":"15ª RISP" , "CASTELO DOS SONHOS":"15ª RISP" , "ITAITUBA":"15ª RISP" , "JACAREACANGA":"15ª RISP" , "NOVO PROGRESSO":"15ª RISP" , "PLACAS":"15ª RISP" , "RUROPOLIS":"15ª RISP" , "TRAIRAO":"15ª RISP"}
        self.__map = {"pk":"pk" , "DELITO":"delito" , "SITUAÇÃO OCORRENCIA":"situacao_ocorrencia" , "PLACA VEICULO":"placa_veiculo" , "UF VEICULO":"uf_veiculo" , "CHASSI VEICULO":"chassi_veiculo" , "MARCA/MODELO":"marca_modelo" , "COR":"cor" , "ANO FABRICAÇÃO":"ano_fabricacao" , "ANO MODELO":"ano_modelo" , "TIPO DE VEICULO":"tipo_veiculo" , "TIPO1":"tipo_veiculo_1" , "TIPO2":"tipo_veiculo_2" , "CATEGORIA":"categoria" , "UF BOP":"uf_bop" , "N    BOP (RECUPERACAO)":"n_bop_recuperacao" , "N   BOP (DECLARACAO)":"n_bop_declaracao" , "DATA REGISTRO":"data_registro" , "HORA REGISTRO":"hora_registro" , "DATA FATO":"data_fato" , "MÊS REGISTRO":"mes_registro" , "MÊS FATO":"mes_fato" , "ANO REGISTRO":"ano_registro" , "ANO FATO SIAC":"ano_fato_siac" , "ANO FATO":"ano_fato" , "DIA DA SEMANA":"dia_semana" , "HORA DO FATO":"hora_fato" , "FAIXA DE HORA":"faixa_hora" , "TEMPO RECUPERAÇÃO (DIAS)":"tempo_recuperacao" , "ENDEREÇO DA RECUPERAÇÃO":"endereco_recuperacao" , "LOCAL DA RECUPERAÇÃO":"local_recuperacao" , "LOCAL DA RECUPERACAO SIAC":"local_recuperacao_siac" , "UF RECUPERAÇÃO":"uf_recuperacao" , "NMBAIRRO":"nmbairro" , "REGIAO":"regiao" , "RISP":"risp"}

    def set_new_columns(self):
        self.__df["pk"] = self.__df.index
        self.__df["TIPO1"] = self.__df["TIPO DE VEICULO"]
        self.__df["TIPO2"] = None
        self.__df["ANO FATO SIAC"] = self.__df["ANO FATO"]
        self.__df["LOCAL DA RECUPERACAO SIAC"] = self.__df["LOCAL DA RECUPERAÇÃO"]
        self.__df["REGIAO"] = self.__df["LOCAL DA RECUPERAÇÃO"].apply(lambda municipio: self.__regiao[municipio] if municipio in self.__regiao.keys() else None)
        self.__df["RISP"] = self.__df["LOCAL DA RECUPERAÇÃO"].apply(lambda municipio: self.__risp[municipio] if municipio in self.__risp.keys() else None)
        self.__df["DATA REGISTRO"] = self.__df["DATA REGISTRO"].apply(lambda x: datetime.datetime.strptime(x,'%d/%m/%Y'))
        self.__df["DATA FATO"] = self.__df["DATA FATO"].apply(lambda x: datetime.datetime.strptime(x,'%d/%m/%Y'))
        return self.__df

    def order_columns(self):
        self.__df = self.__df[["pk","DELITO" , "SITUAÇÃO OCORRENCIA" , "PLACA VEICULO" , "UF VEICULO" , "CHASSI VEICULO" , "MARCA/MODELO" , "COR" , "ANO FABRICAÇÃO" , "ANO MODELO" , "TIPO DE VEICULO" , "TIPO1" , "TIPO2" , "CATEGORIA" , "UF BOP" , "N    BOP (RECUPERACAO)" , "N   BOP (DECLARACAO)" , "DATA REGISTRO" , "HORA REGISTRO" , "DATA FATO" , "MÊS REGISTRO" , "MÊS FATO" , "ANO REGISTRO" , "ANO FATO SIAC" , "ANO FATO" , "DIA DA SEMANA" , "HORA DO FATO" , "FAIXA DE HORA" , "TEMPO RECUPERAÇÃO (DIAS)" , "ENDEREÇO DA RECUPERAÇÃO" , "LOCAL DA RECUPERAÇÃO" , "LOCAL DA RECUPERACAO SIAC" , "UF RECUPERAÇÃO" , "NMBAIRRO" , "REGIAO" , "RISP" ]]
        self.__df = self.__df.rename(columns=self.__map, errors='ignore')
        return self.__df


    def excel_df(self):
        self.__df.to_excel("teste.xlsx",engine='openpyxl')
        
